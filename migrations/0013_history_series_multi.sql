-- history_series_multi: a série de VÁRIOS campos numa varredura só.
--
-- PROBLEMA
-- Depois de 0012 a função ficou ~4x mais barata (5 dias: 8,06 s → 2,1 s quente),
-- mas a tela continuou levando mais de 15 s para desenhar o gráfico.
--
-- A causa não é mais o custo por linha, é o número de VARREDURAS. A tela pede
-- 5 campos e cada um vira uma chamada que relê as MESMAS linhas:
--
--     janela de 7 dias, category "Dados1 Motor Diesel" ... 172.873 linhas
--     x 5 campos ......................................... 864.365 linhas lidas
--
-- Medido contra o banco real, os 5 campos em PARALELO (o que a tela faz):
--
--     RpmMotor ..................  9,53 s  57014
--     TorqueMotor ...............  9,40 s  57014
--     ConsumoInstantaneoDiesel ..  9,79 s  57014
--     TemperaturaMotor ..........  9,85 s  57014
--     PressaoMotor ..............  9,52 s  57014
--
-- Os mesmos 5 campos um a um: 3 passam (2,4 s a 6,9 s) e 2 estouram. Em
-- paralelo eles disputam a mesma CPU do banco, cada um empurra os outros para
-- além do statement timeout (~8 s) e TODOS falham — aí os 5 caem na varredura
-- em Python, que é o que produz os 15 s+ na tela.
--
-- Também medido, e descartado:
--   - fatiar em 1 dia por requisição (35 chamadas): 104 s e 11 falhas. Mais
--     requisições concorrentes pioram, não melhoram.
--   - repetir a chamada que falhou (o retry pega o cache quente): 45 s.
-- A leitura fria custa 3-8x a quente (7 dias: 8,59 s fria contra 2,1 s quente),
-- então qualquer estratégia que multiplique varreduras frias perde.
--
-- SOLUÇÃO
-- Uma varredura, N campos. Recebe os campos como array e devolve uma linha por
-- (campo, bucket). O trabalho de ler e filtrar 172 mil linhas é pago UMA vez em
-- vez de cinco, e a extração do JSONB (que é o custo por linha de 0012) passa a
-- render 5 valores por linha lida.
--
-- `unnest` + `lateral` é o que expande cada linha nos campos pedidos sem reler
-- a tabela: o Postgres varre uma vez e, para cada linha, percorre o array em
-- memória.
--
-- Roda em BLOCO ÚNICO no SQL Editor (só CREATE FUNCTION e GRANT). É uma função
-- NOVA: não substitui history_series, que continua atendendo o caminho de um
-- campo só (drill-down, exportação de um dado). O backend usa esta quando pede
-- vários campos da mesma category e cai na antiga no resto.
--
-- Fuso fixo -03:00 e `received_at` como instante do bucket: mesmos motivos
-- documentados em 0011, inalterados.

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
          -- COALESCE, e não OR: mantém o predicado SARGable quando o parâmetro
          -- é nulo (ver 0011).
          and t.category = coalesce(p_category, t.category)
          and t.received_at >= coalesce(p_since, '-infinity'::timestamptz)
          and t.received_at <= coalesce(p_until, 'infinity'::timestamptz)
    ),
    exploded as (
        -- UMA passada pelas linhas; para cada uma, os campos pedidos saem do
        -- array em memória. `jv` é a única extração de JSONB por (linha, campo),
        -- mesmo contrato de 0012.
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
        date_trunc(p_granularity, ts at time zone '-03:00') at time zone '-03:00'
            as bucket,
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
