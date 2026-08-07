-- Agregação do histórico DENTRO do banco (RPC para a tela de Histórico).
--
-- PROBLEMA
-- A série do gráfico e a da planilha eram montadas no Python: o backend paginava
-- a janela de 1.000 em 1.000 linhas, trazia o valor de cada leitura pela rede e
-- só então calculava min/máx/média por bucket. Para desenhar 315 pontos de um
-- período de 45 dias isso significa ~945 requisições ao PostgREST.
--
-- Medição que motivou este arquivo (945 mil linhas, banco SEM outros
-- consumidores, GET /api/machines/{id}/history?full=true):
--     1 campo, período inteiro .......................   36 s
--     5 campos em paralelo (o que a tela faz) ........  183 s
--
-- O trabalho útil é sempre o mesmo: 315 linhas de resultado. O que custa é
-- trafegar as 945 mil leituras que serão descartadas na agregação.
--
-- SOLUÇÃO
-- `date_trunc` + `group by` fazem a mesma conta onde os dados estão, e devolvem
-- só os buckets. Uma requisição, algumas centenas de linhas na resposta.
--
-- Efeito colateral que importa: min e máx passam a ser os REAIS por construção,
-- sem varrer nada. O `full=true` existia porque agregar no Python obrigava a
-- escolher entre precisão e tempo (o gráfico amostrava, a planilha varria tudo);
-- aqui os dois caminhos são exatos e rápidos, e a diferença entre eles vira
-- apenas a granularidade pedida.
--
-- COMO APLICAR
-- Este arquivo roda em BLOCO ÚNICO: só tem CREATE FUNCTION e GRANT, e ambos
-- funcionam dentro de transação (ao contrário das migrations de índice, que
-- exigem um comando por vez — ver o README desta pasta).
--
-- É `create or replace`, então reaplicar por cima de uma versão anterior é
-- seguro e é o caminho de correção: não precisa apagar a função antes.

-- Fuso dos dispositivos TITAN/THOR. Os buckets são alinhados à borda do
-- calendário LOCAL, não em UTC: alinhar em UTC faria o "dia 28" começar às 21h
-- do dia 27 para o usuário, e clicar num dia traria parte do dia anterior. É o
-- mesmo _DEVICE_TZ de app/application/use_cases.py.
--
-- Escrito como interval fixo (-03:00) e não como nome de zona ('America/
-- Sao_Paulo') de propósito: o _ts gravado pelos devices carrega offset fixo
-- -03:00 (verificado em 100% das linhas da base), e o Brasil não observa mais
-- horário de verão. Usar o nome da zona faria o Postgres aplicar as regras
-- históricas de DST a datas antigas, deslocando buckets do passado em 1 h em
-- relação ao que o payload afirma.

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
            -- Instante do bucket: `received_at`, que é timestamptz NATIVO e
            -- indexado — não o `_ts` do payload.
            --
            -- O Python usa o _ts (relógio da máquina) e cai para received_at, e
            -- a intenção era espelhar isso aqui. Mas o _ts é TEXTO dentro do
            -- JSONB, então usá-lo obriga a um parse de timestamp por linha, e é
            -- ele que derrubava a função: medido, a janela de 1 dia (19 mil
            -- linhas) respondia em 0,3 s, e a de 7 dias (150 mil) estourava o
            -- statement timeout aos 8,3 s. O mesmo recorte lido cru pelo
            -- PostgREST leva 0,58 s, ou seja, o custo era todo do parse.
            --
            -- A diferença entre os dois instantes é o trânsito MQTT, medido em
            -- centenas de milissegundos nesta base (_ts 13:59:58-03:00 contra
            -- received_at 16:59:58.8Z, ou seja, 0,8 s). Isso é irrelevante para
            -- o menor bucket que a tela oferece (o minuto) e some por completo
            -- em hora, dia e mês. Trocar exatidão de sub-segundo por uma função
            -- que responde é o negócio certo aqui — e o caminho de varredura em
            -- Python, que segue disponível, continua usando o _ts para quem
            -- precisar do relógio da máquina.
            t.received_at as ts,
            -- O payload traz número JSON em alguns campos e string em outros
            -- ("RpmMotor": 1862 contra "PressaoMotor": "4.5"). `->>` normaliza
            -- os dois para texto; o cast decide o resto.
            t.payload->>p_field as raw_v,
            -- levado adiante para o teste de tipo do filtro seguinte
            t.payload as t_payload
        from public.telemetry t
        where t.machine_id = p_machine_id
          -- COALESCE, e não `(p_category is null or t.category = p_category)`.
          -- A forma com OR parece equivalente e não é: o planejador não sabe em
          -- tempo de planejamento se o parâmetro será nulo, então não consegue
          -- transformar o OR num acesso por índice e cai em Seq Scan sobre a
          -- tabela inteira. Medido: a janela de UMA HORA, que responde em 0,58 s
          -- num select normal, estourava o statement timeout aos 8,8 s dentro
          -- da função.
          --
          -- Comparar a coluna com ela mesma quando o parâmetro é nulo mantém o
          -- predicado SARGable (o índice continua utilizável) e o resultado
          -- idêntico: `t.category = t.category` é verdadeiro para toda linha com
          -- category não nula, que é o universo que interessa aqui.
          and t.category = coalesce(p_category, t.category)
          -- O filtro de tempo é por received_at, e não pelo ts calculado: é ele
          -- que tem índice (ver 0007). A diferença entre os dois é o trânsito
          -- da mensagem, irrelevante ante a menor granularidade (o minuto).
          --
          -- Mesmo motivo do COALESCE acima. Os limites nulos viram as bordas do
          -- tipo, que não excluem linha nenhuma e continuam sendo um range scan.
          and t.received_at >= coalesce(p_since, '-infinity'::timestamptz)
          and t.received_at <= coalesce(p_until, 'infinity'::timestamptz)
    ),
    numeric_only as (
        select
            ts,
            raw_v::double precision as v
        from base
        where raw_v is not null
          -- Descarta o que não é medição numérica ANTES do cast: um único valor
          -- não numérico abortaria a consulta inteira com "invalid input syntax
          -- for type double precision".
          --
          -- O teste é por TIPO do JSONB, não por regex sobre o texto. A versão
          -- com regex era o gargalo da função: medido nesta base numa janela de
          -- 7 dias (150 mil linhas), 2.540 ms com regex contra 403 ms sem ele —
          -- 6x. O regex roda uma vez por linha sobre o valor já extraído, e a
          -- extração do JSONB é justamente o que domina o custo aqui.
          --
          -- `jsonb_typeof = 'number'` cobre o caso real (verificado: os cinco
          -- campos de métrica chegam como número JSON em 100% da amostra). O
          -- ramo de string existe para os data_types que gravam "4.5" com
          -- aspas; ali o regex ainda roda, mas só sobre as linhas daquele tipo,
          -- e não sobre todas.
          and (
                jsonb_typeof(t_payload->p_field) = 'number'
             or (
                    jsonb_typeof(t_payload->p_field) = 'string'
                and raw_v ~ '^[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$'
                )
          )
    )
    select
        -- date_trunc no fuso do device, devolvido como timestamptz: o Postgres
        -- converte para UTC no wire, e o backend só faz epoch * 1000.
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

-- A RPC é chamada com a mesma service key do resto do backend (o PostgREST
-- exige o privilégio explícito para expor a função em /rpc).
grant execute on function public.history_series(
    uuid, text, text, text, timestamptz, timestamptz
) to anon, authenticated, service_role;

-- ÍNDICE DE APOIO
-- A agregação lê payload->>p_field de cada linha da janela. O filtro
-- (machine_id, category, received_at) já é servido pelo índice de 0007, que
-- é o que mantém a leitura restrita à janela; o custo restante é a extração do
-- campo, que nenhum índice evita sem uma coluna gerada por campo.
--
-- Não criamos índice de expressão por campo de propósito: são 5 campos hoje e
-- eles mudam conforme o data_type do device, então seria manutenção sem fim
-- para um ganho que a redução de tráfego já entrega.
