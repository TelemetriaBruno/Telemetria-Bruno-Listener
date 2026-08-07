-- history_series: uma extração de JSONB por linha, em vez de três.
--
-- PROBLEMA
-- A função de 0011 respondia dentro do timeout quando foi escrita, e voltou a
-- estourar conforme a base cresceu. Medido no banco real (máquina
-- 4b47ad8f, campo RpmMotor, granularidade hora, 369.913 linhas em ~6,5 dias):
--
--     1 dia  (53 mil linhas) ....................  2,80 s
--     2 dias (107 mil) .........................  3,55 s
--     4 dias (214 mil) .........................  7,36 s
--     5 dias ...................................  8,06 s
--     6 dias ................................... falha (57014)
--     7 dias (o que a tela pede) ............... falha (57014)
--
-- O tempo cresce com o número de LINHAS, não com o de buckets (7 dias em
-- granularidade `day` produz 8 buckets e ainda assim leva 5,08 s). É custo por
-- linha, e não falta de índice: o mesmo recorte filtrado por
-- (machine_id, category, received_at) volta cru do PostgREST em 0,21 s.
--
-- A janela de 7 dias não é um período grande: é a BASE INTEIRA desta máquina.
-- O caminho rápido falhava justamente no caso mais comum da tela, e cada
-- pedido gastava ~9 s batendo no timeout antes de cair na varredura em Python
-- (36 s por campo), que é o que deixava o Histórico lento.
--
-- CAUSA
-- O corpo de 0011 toca o payload TRÊS vezes por linha:
--
--     t.payload->>p_field                      -- valor como texto
--     jsonb_typeof(t_payload->p_field) = ...   -- teste de tipo (2x, no OR)
--
-- Cada `->` / `->>` desserializa o JSONB e procura a chave de novo; é a
-- operação que o próprio 0011 identificou como dominante ao medir o regex
-- (2.540 ms contra 403 ms). Reduzir o número de extrações ataca exatamente
-- esse custo.
--
-- SOLUÇÃO
-- Extrair `payload->p_field` UMA vez por linha, como jsonb, e derivar dele
-- tanto o teste de tipo quanto o valor. O resultado é idêntico ao de 0011:
-- mesmos buckets, mesmo min/máx/média, mesma contagem.
--
-- É `create or replace`: reaplicar por cima de 0011 é o caminho de correção,
-- não precisa apagar a função antes. Roda em BLOCO ÚNICO no SQL Editor (só
-- CREATE FUNCTION e GRANT, ambos válidos dentro de transação).
--
-- O fuso fixo -03:00 e a escolha de `received_at` como instante do bucket são
-- de 0011 e seguem valendo pelos mesmos motivos documentados lá.

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
            t.received_at as ts,
            -- A ÚNICA extração do payload. `->` (e não `->>`) preserva o tipo
            -- JSON, que é o que o filtro seguinte precisa para decidir sem
            -- tocar no documento outra vez.
            t.payload -> p_field as jv
        from public.telemetry t
        where t.machine_id = p_machine_id
          -- COALESCE, e não OR: mantém o predicado SARGable quando o parâmetro
          -- é nulo. Ver 0011 para a medição que motivou esta forma.
          and t.category = coalesce(p_category, t.category)
          and t.received_at >= coalesce(p_since, '-infinity'::timestamptz)
          and t.received_at <= coalesce(p_until, 'infinity'::timestamptz)
    ),
    numeric_only as (
        select
            ts,
            -- #>> '{}' devolve o texto de um valor jsonb escalar sem reabrir o
            -- documento: `jv` já é o campo isolado. Para número JSON dá o
            -- literal ("1862"), para string dá o conteúdo sem aspas ("4.5").
            (jv #>> '{}')::double precision as v
        from base
        where jv is not null
          -- Mesmo contrato de 0011: descarta o não numérico ANTES do cast, ou
          -- uma linha inválida aborta a consulta inteira. A diferença é que
          -- `jsonb_typeof` agora lê a variável já extraída, sem novo acesso ao
          -- payload. O regex continua restrito ao ramo string, que é raro.
          and (
                jsonb_typeof(jv) = 'number'
             or (
                    jsonb_typeof(jv) = 'string'
                and (jv #>> '{}') ~ '^[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$'
                )
          )
    )
    select
        date_trunc(p_granularity, ts at time zone '-03:00') at time zone '-03:00'
            as bucket,
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
