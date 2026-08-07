-- history_series / history_series_multi: fuso por INTERVAL, não por string.
--
-- PROBLEMA
-- O filtro "30 dias" (e "7 dias", e "Ontem") mostrava "Sem leituras de RPM do
-- Motor" numa máquina que tem 6.779 leituras na janela. Medido na ATLAS
-- AT0092 (30c20dae), campo RpmMotor, category "Dados1 Motor Diesel", cuja base
-- inteira cabe em UM dia local (06/08/2026 00:06 a 15:04 no fuso -03:00):
--
--     granularity=hour ... 5 buckets com dado ....... correto
--     granularity=day  ... 1 bucket, e o backend o descarta
--
-- O backend lê 6.779 leituras e devolve 30 pontos todos com count=0: a
-- agregação acontece, mas nenhum bucket casa com as bordas do eixo.
--
-- CAUSA
-- `AT TIME ZONE 'texto'` segue a convenção POSIX quando recebe uma STRING de
-- offset: em POSIX o sinal é INVERTIDO, então '-03:00' significa UTC+3, não
-- UTC-3. A expressão fazia a ida e a volta com o sinal trocado nas duas
-- pontas, deslocando o bucket em 6 horas (2 x 3h):
--
--     leitura  2026-08-06 03:06 UTC  (= 06/08 00:06 em -03:00)
--     bucket correto ........ 2026-08-06 03:00+00  (06/08 00:00 local)
--     bucket produzido ...... 2026-08-05 21:00+00  (05/08 18:00 local)
--
-- Por que `hour` escapava e `day` não: truncar em HORA depois de um
-- deslocamento de horas inteiras cai numa borda de hora válida de qualquer
-- jeito, então o bucket continuava casando. Truncar em DIA move o bucket para
-- as 18:00 do dia ANTERIOR, que não é borda de dia nenhuma — e o
-- `_bucket_edges` do backend, que gera as bordas corretamente no fuso do
-- dispositivo, não encontra a chave e emite count=0 para todos os pontos.
--
-- Isso também explica por que "Tudo" parecia funcionar em outra máquina: com
-- várias semanas de dado, o eixo cai em `month`/`day` sobre buckets que
-- existiam por acaso na vizinhança; com UM dia de dado, o único bucket erra o
-- alvo e a série inteira zera.
--
-- SOLUÇÃO
-- `AT TIME ZONE interval '-03:00'`. Com um INTERVAL o Postgres usa o sinal
-- aritmético (ISO), sem a inversão do POSIX, e a ida e a volta ficam
-- simétricas. Continua sendo offset FIXO, não nome de zona: o motivo original
-- de 0011 segue valendo — `America/Sao_Paulo` aplicaria DST histórico e
-- deslocaria buckets antigos em 1h, enquanto o `_ts` do dispositivo grava
-- offset fixo em 100% das linhas.
--
-- Verificação (mesma máquina, mesma janela, depois de aplicar):
--     granularity=day deve devolver bucket 2026-08-06T03:00:00+00:00
--     que é 06/08 00:00 em -03:00, e casa com _bucket_edges do backend.
--
-- Ambas são `create or replace`: reaplicar por cima de 0012/0013 é o caminho,
-- não precisa apagar antes. Roda em BLOCO ÚNICO no SQL Editor (só CREATE
-- FUNCTION e GRANT, válidos dentro de transação).

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
        where t.machine_id = p_machine_id
          and t.category = coalesce(p_category, t.category)
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

grant execute on function public.history_series(
    uuid, text, text, text, timestamptz, timestamptz
) to anon, authenticated, service_role;


create or replace function public.history_series_multi(
    p_machine_id   uuid,
    p_fields       text[],
    p_granularity  text    default 'hour',
    p_category     text    default null,
    p_since        timestamptz default null,
    p_until        timestamptz default null
)
returns table (
    field    text,
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
            t.received_at as ts,
            t.payload     as payload
        from public.telemetry t
        where t.machine_id = p_machine_id
          and t.category = coalesce(p_category, t.category)
          and t.received_at >= coalesce(p_since, '-infinity'::timestamptz)
          and t.received_at <= coalesce(p_until, 'infinity'::timestamptz)
    ),
    exploded as (
        select
            b.ts,
            f.field,
            b.payload -> f.field as jv
        from base b
        cross join lateral unnest(p_fields) as f(field)
    ),
    numeric_only as (
        select
            ts,
            field,
            (jv #>> '{}')::double precision as v
        from exploded
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
        field,
        date_trunc(p_granularity, ts at time zone interval '-03:00')
            at time zone interval '-03:00' as bucket,
        min(v)   as min_v,
        max(v)   as max_v,
        avg(v)   as avg_v,
        count(*) as n
    from numeric_only
    group by 1, 2
    order by 1, 2;
$$;

grant execute on function public.history_series_multi(
    uuid, text[], text, text, timestamptz, timestamptz
) to anon, authenticated, service_role;
