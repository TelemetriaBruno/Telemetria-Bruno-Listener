-- telemetry_rollup_hour: o histórico pré-agregado por hora.
--
-- PROBLEMA
-- O Histórico abre em "Todo o período", e cada abertura fazia o banco reagregar
-- todas as leituras da máquina naquela janela: ler a linha, abrir o JSONB, extrair
-- o campo. O custo cresce com o número de leituras, não com os pontos do gráfico.
-- Medido em 0013: 7 dias de "Dados1 Motor Diesel" (172.873 linhas) levam 8,59 s
-- com o cache frio e 2,1 s quente, POR CAMPO, cerca de 1,2 s por dia de dado. Um
-- gráfico de 12 pontos sobre meses de dado passa de minutos, e só piora com o tempo.
--
-- SOLUÇÃO
-- Guardar min, máx, soma e contagem por (máquina, campo, tópico, hora). O gráfico
-- passa a custar o número de HORAS do período, sem tocar nas leituras: um ano são
-- 8.760 linhas, lidas em ordem de um único índice. Soma e contagem, e não a média,
-- porque somar horas em dias e meses exige a média ponderada.
--
-- Só horas FECHADAS entram. A hora corrente continua vindo das leituras (é pouca
-- coisa), o backend junta as duas partes, e assim cada hora é gravada uma vez em
-- vez de reescrita a cada minuto.
--
-- Quem mantém é o backend: ele chama telemetry_rollup_refresh a cada minuto. Na
-- primeira vez processa o histórico inteiro, em lotes. Até terminar, coberto_ate
-- fica nulo e o backend segue agregando as leituras, como antes da migration.
--
-- A hora é truncada em UTC e o painel usa -03:00: como o deslocamento é de horas
-- inteiras, a borda da hora é a mesma nos dois fusos, e dia e mês são montados na
-- leitura com o mesmo fuso fixo de 0014.
--
-- Roda em transação, sem passos: só cria objetos novos e troca o corpo de
-- history_series.
--
-- ROLLBACK (no rodapé).

create table if not exists public.telemetry_rollup_hour (
    machine_id uuid             not null references public.machines (id) on delete cascade,
    field      text             not null,
    category   text             not null,
    bucket     timestamptz      not null,
    min_v      double precision not null,
    max_v      double precision not null,
    sum_v      double precision not null,
    n          bigint           not null,
    -- INCLUDE: a leitura do gráfico sai toda do índice, sem ir à tabela. Os campos
    -- de uma mesma hora são gravados juntos, então as linhas de UM campo ficam
    -- espalhadas pela tabela, enquanto no índice ficam vizinhas.
    constraint telemetry_rollup_hour_pk
        primary key (machine_id, field, category, bucket)
        include (min_v, max_v, sum_v, n)
);

create table if not exists public.telemetry_rollup_state (
    singleton     boolean     primary key default true check (singleton),
    -- última linha de telemetry já somada. Por id, e não por data, porque é o id
    -- que garante que nenhuma linha fica para trás: uma leitura guardada offline
    -- pelo Listener chega depois, com data antiga e id novo, e ainda é somada.
    last_id       bigint      not null default 0,
    -- toda leitura com received_at anterior a isto já está no rollup. Nulo até a
    -- carga inicial terminar: uma cobertura parcial daria um gráfico errado.
    covered_until timestamptz,
    updated_at    timestamptz not null default now()
);

insert into public.telemetry_rollup_state (singleton) values (true)
on conflict (singleton) do nothing;


-- O valor numérico de um campo do JSONB, ou null. Nunca levanta erro: um valor
-- impossível de converter parado num lote travaria o rollup para sempre. O limite
-- de 40 caracteres e o expoente de até 2 dígitos são o que impede o "fora do
-- alcance" de double precision (1e999, ou um número de 400 dígitos).
create or replace function public.telemetry_numeric(j jsonb)
returns double precision
language sql
immutable
parallel safe
as $$
    select case
        when length(j #>> '{}') > 40 then null
        when jsonb_typeof(j) = 'number' then (j #>> '{}')::double precision
        when jsonb_typeof(j) = 'string'
             and (j #>> '{}') ~ '^[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]{1,2})?$'
            then (j #>> '{}')::double precision
    end
$$;


-- Soma o próximo lote de leituras. Devolve até onde foi, se chegou ao fim e até
-- quando o rollup cobre. Lote pequeno de propósito: o Listener grava na mesma
-- tabela, e a carga inicial não pode disputar o banco com ele.
--
-- SECURITY DEFINER: as tabelas de rollup ficam só com o dono e o papel do PostgREST
-- não precisa de permissão nelas, nem o nome dele aparece aqui. Vale para todas as
-- funções abaixo.
--
-- Recua 2 minutos antes de escolher a hora: um id só é visível depois do commit,
-- então uma linha em voo poderia ser ultrapassada por outra de id maior e nunca
-- mais ser somada. 2 minutos é mais que qualquer gravação em voo.
create or replace function public.telemetry_rollup_refresh(p_batch integer default 5000)
returns table (ultimo_id bigint, em_dia boolean, coberto_ate timestamptz)
language plpgsql
security definer
set search_path = public, pg_temp
set timezone = 'UTC'
as $$
declare
    v_cutoff timestamptz := date_trunc('hour', now() - interval '2 minutes');
    v_last   bigint;
    v_max    bigint;
    v_corte  bigint;
    v_lidos  bigint;
    v_to     bigint;
    v_em_dia boolean;
begin
    -- um por vez; se outro processo já está somando, este sai sem fazer nada
    select s.last_id into v_last
      from telemetry_rollup_state s
     where s.singleton
       for update skip locked;
    if not found then
        return;
    end if;

    select count(*), max(l.id), min(l.id) filter (where l.received_at >= v_cutoff)
      into v_lidos, v_max, v_corte
      from (
        select t.id, t.received_at
          from telemetry t
         where t.id > v_last
         order by t.id
         limit p_batch
      ) l;

    -- para na primeira leitura de hora ainda aberta, mesmo que venha outra antiga
    -- depois dela: o id é o que garante que nada se perde
    v_to := case when v_corte is not null then v_corte - 1 else v_max end;
    v_em_dia := v_corte is not null or v_lidos < p_batch;

    if v_to is not null and v_to > v_last then
        insert into telemetry_rollup_hour as r
               (machine_id, field, category, bucket, min_v, max_v, sum_v, n)
        select t.machine_id,
               kv.key,
               t.category,
               date_trunc('hour', t.received_at),
               min(x.v),
               max(x.v),
               sum(x.v),
               count(*)
          from telemetry t
         cross join lateral jsonb_each(
                 case when jsonb_typeof(t.payload) = 'object' then t.payload else '{}'::jsonb end
               ) as kv(key, value)
         cross join lateral (select telemetry_numeric(kv.value) as v) as x
         where t.id > v_last
           and t.id <= v_to
           and t.category is not null
           and kv.key not in ('_ts', '_raw', 'raw_hex', 'DataHora', 'HoraData')
           and x.v is not null
         group by t.machine_id, kv.key, t.category, date_trunc('hour', t.received_at)
        on conflict (machine_id, field, category, bucket) do update
           set min_v = least(r.min_v, excluded.min_v),
               max_v = greatest(r.max_v, excluded.max_v),
               sum_v = r.sum_v + excluded.sum_v,
               n     = r.n + excluded.n;
    end if;

    update telemetry_rollup_state s
       set last_id = coalesce(v_to, v_last),
           covered_until = case when v_em_dia then v_cutoff else s.covered_until end,
           updated_at = now()
     where s.singleton;

    return query
        select s.last_id, v_em_dia, s.covered_until
          from telemetry_rollup_state s
         where s.singleton;
end
$$;


-- Até quando o rollup cobre. O backend só lê do rollup até este instante.
create or replace function public.telemetry_rollup_coverage()
returns timestamptz
language sql
stable
security definer
set search_path = public, pg_temp
as $$
    select s.covered_until from telemetry_rollup_state s where s.singleton
$$;


-- Mesmo contrato de history_series, lendo do rollup. p_until é EXCLUSIVO: é uma
-- borda de hora, e a hora que começa nela fica de fora.
create or replace function public.history_series_rollup(
    p_machine_id   uuid,
    p_field        text,
    p_granularity  text,
    p_category     text        default null,
    p_since        timestamptz default null,
    p_until        timestamptz default null
)
returns table (
    bucket   timestamptz,
    min_v    double precision,
    max_v    double precision,
    avg_v    double precision,
    n        bigint
)
language sql
stable
security definer
set search_path = public, pg_temp
as $$
    select
        date_trunc(p_granularity, r.bucket at time zone interval '-03:00')
            at time zone interval '-03:00' as bucket,
        min(r.min_v),
        max(r.max_v),
        sum(r.sum_v) / sum(r.n)::double precision,
        sum(r.n)::bigint
    from telemetry_rollup_hour r
    where r.machine_id = p_machine_id
      and r.field = p_field
      and (p_category is null or r.category = p_category)
      and (p_since is null or r.bucket >= p_since)
      and (p_until is null or r.bucket < p_until)
    group by 1
    order by 1
$$;


-- Os campos que a máquina já publicou, em qualquer época. Salta de par em par pelo
-- índice (como telemetry_latest em 0022): uma busca por campo, em vez de percorrer
-- todas as horas de todos os campos.
create or replace function public.history_fields(p_machine_id uuid)
returns table (field text, category text)
language sql
stable
security definer
set search_path = public, pg_temp
as $$
    with recursive s(field, category) as (
        (
            select r.field, r.category
              from telemetry_rollup_hour r
             where r.machine_id = p_machine_id
             order by r.field, r.category
             limit 1
        )
        union all
        select prox.field, prox.category
          from s
         cross join lateral (
            select r.field, r.category
              from telemetry_rollup_hour r
             where r.machine_id = p_machine_id
               and (r.field, r.category) > (s.field, s.category)
             order by r.field, r.category
             limit 1
         ) prox
    )
    select s.field, s.category from s
$$;


-- Leituras apagadas pelo backoffice. Máquina inteira: sai só o rollup dela.
create or replace function public.telemetry_rollup_forget(p_machine_id uuid)
returns void
language plpgsql
security definer
set search_path = public, pg_temp
as $$
begin
    perform 1 from telemetry_rollup_state s where s.singleton for update;
    delete from telemetry_rollup_hour r where r.machine_id = p_machine_id;
end
$$;


-- Período ou cliente apagados: refaz tudo do zero. Um recorte no meio de uma hora
-- não dá para descontar de uma soma, e a linha da telemetria guarda o cliente de
-- quando foi gravada, então nem dá para saber de quais máquinas ela saiu. É uma
-- operação rara, e enquanto o rollup refaz o histórico agrega as leituras.
create or replace function public.telemetry_rollup_reset()
returns void
language plpgsql
security definer
set search_path = public, pg_temp
as $$
begin
    perform 1 from telemetry_rollup_state s where s.singleton for update;
    truncate telemetry_rollup_hour;
    update telemetry_rollup_state s
       set last_id = 0, covered_until = null, updated_at = now()
     where s.singleton;
end
$$;


-- history_series agora só atende o trecho que o rollup não cobre: a hora corrente,
-- e os degraus de minuto e segundo. Mesma assinatura, mesmo resultado.
--
-- `t.category = coalesce(p_category, t.category)` não usa o índice (machine_id,
-- category, received_at), porque a coluna aparece dos dois lados da comparação: o
-- banco lia TODOS os tópicos da máquina na janela e descartava os outros depois de
-- ler cada linha. Separar os dois casos entrega a categoria direto ao índice.
create or replace function public.history_series(
    p_machine_id   uuid,
    p_field        text,
    p_granularity  text    default 'hour',
    p_category     text    default null,
    p_since        timestamptz default null,
    p_until        timestamptz default null
)
returns table (
    bucket   timestamptz,
    min_v    double precision,
    max_v    double precision,
    avg_v    double precision,
    n        bigint
)
language sql
stable
parallel safe
as $$
    with base as (
        select
            t.received_at        as ts,
            t.payload -> p_field as jv
        from public.telemetry t
        where p_category is not null
          and t.machine_id = p_machine_id
          and t.category = p_category
          and t.received_at >= coalesce(p_since, '-infinity'::timestamptz)
          and t.received_at <= coalesce(p_until, 'infinity'::timestamptz)
        union all
        select
            t.received_at        as ts,
            t.payload -> p_field as jv
        from public.telemetry t
        where p_category is null
          and t.machine_id = p_machine_id
          and t.category is not null
          and t.received_at >= coalesce(p_since, '-infinity'::timestamptz)
          and t.received_at <= coalesce(p_until, 'infinity'::timestamptz)
    ),
    numeric_only as (
        select
            ts,
            (jv #>> '{}')::double precision as v
        from base
        where jv is not null
          and (
                jsonb_typeof(jv) = 'number'
             or (
                    jsonb_typeof(jv) = 'string'
                and (jv #>> '{}') ~ '^[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$'
                )
          )
    )
    select
        date_trunc(p_granularity, ts at time zone interval '-03:00')
            at time zone interval '-03:00' as bucket,
        min(v)   as min_v,
        max(v)   as max_v,
        avg(v)   as avg_v,
        count(*) as n
    from numeric_only
    group by 1
    order by 1;
$$;


-- Para todos, como em 0022: os papéis do Supabase podem não existir no RDS.
grant execute on function public.telemetry_rollup_refresh(integer) to public;
grant execute on function public.telemetry_rollup_coverage() to public;
grant execute on function public.history_series_rollup(uuid, text, text, text, timestamptz, timestamptz) to public;
grant execute on function public.history_fields(uuid) to public;
grant execute on function public.telemetry_rollup_forget(uuid) to public;
grant execute on function public.telemetry_rollup_reset() to public;

-- O PostgREST guarda as funções em cache: sem isto as rotas respondem PGRST202 até
-- ele reiniciar.
notify pgrst, 'reload schema';

-- ROLLBACK
--   O backend volta a agregar as leituras sozinho quando as funções somem. O corpo
--   antigo de history_series está em 0014.
--     drop function if exists public.telemetry_rollup_refresh(integer);
--     drop function if exists public.telemetry_rollup_coverage();
--     drop function if exists public.history_series_rollup(uuid, text, text, text, timestamptz, timestamptz);
--     drop function if exists public.history_fields(uuid);
--     drop function if exists public.telemetry_rollup_forget(uuid);
--     drop function if exists public.telemetry_rollup_reset();
--     drop function if exists public.telemetry_numeric(jsonb);
--     drop table if exists public.telemetry_rollup_state;
--     drop table if exists public.telemetry_rollup_hour;
