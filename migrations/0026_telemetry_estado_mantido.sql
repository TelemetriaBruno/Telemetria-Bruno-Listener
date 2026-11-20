-- telemetry.held_until / telemetry.repeats: leitura repetida não vira linha nova.
--
-- PROBLEMA
-- O Listener gravava uma linha por mensagem, mesmo quando ela repetia a anterior
-- do tópico. Na BF6969 (17–18/11/2026) 97,7% das 20.443 mensagens eram iguais à
-- anterior do mesmo tópico, e a planilha Rev2 do firmware manda os tópicos da
-- máquina a cada 300–950 ms: cerca de 31 mil linhas por hora por máquina, quase
-- todas repetindo o mesmo estado (parâmetros, horímetros, esteiras paradas, o
-- vetor de alarmes).
--
-- PEDIDO (20/11/2026)
-- Valor igual ao anterior só aparece ao vivo no painel; o banco registra que o
-- valor FICOU naquele estado até mudar. O histórico não pode mudar: se a bateria
-- marcou 24 V das 14h às 15h e caiu para 12 V, o gráfico mostra uma hora em 24 V e
-- a queda, e não uma descida aos poucos nem um buraco.
--
-- SOLUÇÃO
-- Cada linha passa a ser um ESTADO:
--   received_at  quando o valor chegou pela primeira vez (como sempre);
--   held_until   a chegada da última leitura igual (NULL = leitura única);
--   repeats      quantas leituras iguais vieram depois da primeira.
-- O Listener (app/domain/hold.py) não deixa um estado atravessar a hora cheia, e
-- alarme continua uma linha por mensagem.
--
-- Quem lê recompõe as leituras: um estado com repeats = N vale 1 + N leituras
-- espaçadas por igual entre received_at e held_until. Com isso o gráfico, a média,
-- o mínimo, o máximo e a contagem saem iguais aos de antes:
--   - history_series espalha as repetidas pelo período antes de agrupar;
--   - o rollup por hora soma o valor e a contagem com o peso 1 + repeats;
--   - linhas antigas têm repeats = 0 e held_until NULL: valem uma leitura, como
--     sempre valeram.
-- O backend faz o mesmo nas leituras que agrega em Python (varredura, consumo da
-- frota, trajeto, leituras cruas) e o painel usa held_until como a hora da
-- última leitura.
--
-- Ordem de publicação: ESTA migration, depois backend e painel, por último o
-- Listener. O Listener só passa a segurar as repetidas quando encontra as colunas
-- e a permissão de atualizá-las; até lá grava toda leitura, como antes.
--
-- Roda em transação, sem passos. ADD COLUMN com default constante não reescreve a
-- tabela (Postgres 11+): é instantâneo mesmo com milhões de linhas.

alter table public.telemetry add column if not exists held_until timestamptz;
alter table public.telemetry add column if not exists repeats integer not null default 0;

comment on column public.telemetry.held_until is
    'Chegada da última leitura igual a esta (estado mantido). NULL = leitura única.';
comment on column public.telemetry.repeats is
    'Leituras iguais que vieram depois da primeira e não viraram linha (estado mantido).';

-- O Listener atualiza as duas colunas na linha do estado. Só elas: o resto da
-- linha continua só de inserção. O papel pode não existir no RDS (ver 0022/0023).
do $$
begin
    if exists (select 1 from pg_roles where rolname = 'service_role') then
        grant update (held_until, repeats) on public.telemetry to service_role;
    end if;
end
$$;


-- Rollup por hora: cada linha pesa 1 + repeats. A linha de estado nunca passa da
-- hora em que começou (regra do Listener), então cabe inteira no bucket da hora.
--
-- A margem até a hora fechar entrar no rollup sobe de 2 para 5 minutos: a linha
-- do estado só fica completa quando o Listener grava o fim dele, no máximo ~10 s
-- depois da última leitura igual, e 5 minutos cobrem também uma fila de gravação
-- atrasada. O trecho a mais continua vindo das leituras, como a hora corrente.
create or replace function public.telemetry_rollup_refresh(p_batch integer default 5000)
returns table (ultimo_id bigint, em_dia boolean, coberto_ate timestamptz)
language plpgsql
security definer
set search_path = public, pg_temp
set timezone = 'UTC'
as $$
declare
    v_cutoff timestamptz := date_trunc('hour', now() - interval '5 minutes');
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
               -- o estado vale 1 + repeats leituras do mesmo valor
               sum(x.v * (1 + t.repeats)),
               sum(1 + t.repeats)
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


-- history_series (o trecho que o rollup não cobre: hora corrente, minuto,
-- segundo): as leituras repetidas voltam a existir na consulta, espaçadas por
-- igual entre received_at e held_until, antes de agrupar. Mesma assinatura e,
-- sobre as linhas antigas (repeats = 0), o mesmo resultado de 0023.
--
-- A janela começa uma hora antes do pedido: um estado que começou antes de
-- p_since pode durar até dentro dela (nunca mais que uma hora, pela regra do
-- Listener). As leituras recompostas são cortadas na janela pedida depois.
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
            t.held_until         as te,
            t.repeats            as r,
            t.payload -> p_field as jv
        from public.telemetry t
        where p_category is not null
          and t.machine_id = p_machine_id
          and t.category = p_category
          and t.received_at >= coalesce(p_since - interval '1 hour', '-infinity'::timestamptz)
          and t.received_at <= coalesce(p_until, 'infinity'::timestamptz)
        union all
        select
            t.received_at        as ts,
            t.held_until         as te,
            t.repeats            as r,
            t.payload -> p_field as jv
        from public.telemetry t
        where p_category is null
          and t.machine_id = p_machine_id
          and t.category is not null
          and t.received_at >= coalesce(p_since - interval '1 hour', '-infinity'::timestamptz)
          and t.received_at <= coalesce(p_until, 'infinity'::timestamptz)
    ),
    numeric_only as (
        select
            ts,
            greatest(coalesce(te, ts), ts) as te,
            r,
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
    ),
    leituras as (
        -- a primeira leitura de cada estado
        select ts, v from numeric_only
        union all
        -- as iguais que vieram depois, no período em que o valor se manteve
        select n.ts + (n.te - n.ts) * (i::double precision / n.r), n.v
          from numeric_only n
         cross join lateral generate_series(1, n.r) as i
         where n.r > 0
    )
    select
        date_trunc(p_granularity, ts at time zone interval '-03:00')
            at time zone interval '-03:00' as bucket,
        min(v)   as min_v,
        max(v)   as max_v,
        avg(v)   as avg_v,
        count(*) as n
    from leituras
    where ts >= coalesce(p_since, '-infinity'::timestamptz)
      and ts <= coalesce(p_until, 'infinity'::timestamptz)
    group by 1
    order by 1;
$$;

grant execute on function public.telemetry_rollup_refresh(integer) to public;
grant execute on function public.history_series(uuid, text, text, text, timestamptz, timestamptz) to public;

-- history_series_multi (0013/0014) fica como está: nenhuma parte do sistema a chama.

-- O PostgREST guarda o esquema em cache: sem isto as colunas novas não aparecem
-- na API até ele reiniciar.
notify pgrst, 'reload schema';

-- CONFERIR
--   select column_name, data_type, column_default
--     from information_schema.columns
--    where table_name = 'telemetry' and column_name in ('held_until', 'repeats');
--   -- depois de o Listener novo rodar alguns minutos:
--   select category, count(*) as linhas, sum(1 + repeats) as leituras
--     from telemetry where received_at > now() - interval '15 minutes'
--    group by category order by leituras desc;
--
-- ROLLBACK
--   Primeiro o Listener: HOLD_REPEATED=false no .env e reiniciar (volta a gravar
--   toda leitura). As colunas podem ficar: com repeats = 0 em toda linha nova,
--   leitores e funções novos dão o mesmo resultado dos antigos. As linhas de
--   estado já gravadas continuam corretas só para quem sabe lê-las, então não
--   apague as colunas sem antes voltar backend e painel.
--     Funções antigas: telemetry_rollup_refresh e history_series em 0023.
