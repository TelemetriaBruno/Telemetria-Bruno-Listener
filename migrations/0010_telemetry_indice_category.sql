-- Índice de `category` sem máquina, para o painel de tempo real.
--
-- PROBLEMA
-- A tela inicial carrega as últimas leituras de cada data_type da frota:
--
--     select * from telemetry where category = :c order by id desc limit 20
--
-- Os índices de 001 começam todos por machine_id, então uma consulta que
-- filtra SÓ por category não usa nenhum deles. O Postgres varre a chave
-- primária de trás para frente procurando as 20 linhas daquela categoria, e
-- as categorias esparsas (Horímetros chega a cada 10-16 min, contra 1,2 s dos
-- dados de motor) obrigam a percorrer centenas de milhares de linhas antes de
-- juntar 20. Passa do statement timeout do Supabase e volta 57014, que o
-- backend converte em 502 e o painel mostra como "Bad Gateway".
--
-- Medição que motivou este arquivo (tabela com 945 mil linhas):
--     sem filtro, order=id.desc .......................  0,70 s
--     category + order=received_at.desc ...............  5,80 s
--     category + order=id.desc (o que a tela faz) ..... falha em 9,43 s
--
-- É intermitente de propósito enganoso: sob carga leve responde em 5-9 s e o
-- usuário só acha lento; sob carga real estoura o timeout e vira erro.

-- COMO APLICAR (leia antes de colar)
--
-- Este arquivo NÃO roda de uma vez. `CREATE INDEX CONCURRENTLY` não pode rodar
-- dentro de uma transação, e o SQL Editor do Supabase envolve em UMA transação
-- tudo o que está na aba — não um statement por vez. Colar o arquivo inteiro
-- devolve:
--
--     ERROR: 25001: CREATE INDEX CONCURRENTLY cannot run inside a transaction block
--
-- O erro não indica problema no índice: é só o modo de envio. Rode um PASSO por
-- vez, deixando na aba SOMENTE aquele comando (apague o resto ou selecione a
-- linha e use "Run selected"). Cada passo leva de segundos a alguns minutos
-- numa tabela grande, e `concurrently` existe justamente para que a tabela seja
-- lida e escrita normalmente enquanto o índice é construído.
--
-- Alternativa sem passo a passo: o índice sem `concurrently` roda em bloco
-- único, mas BLOQUEIA escritas na tabela até terminar — o Listener pararia de
-- gravar telemetria nesse intervalo. Só vale numa janela de manutenção.

-- ---------------------------------------------------------------------------
-- PASSO 1 de 3
-- (category, id): resolve o filtro e já entrega a ordem que a paginação usa,
-- então o Postgres lê as 20 linhas direto do índice, sem ordenar depois.
-- ---------------------------------------------------------------------------
create index concurrently if not exists telemetry_category_id_idx
    on public.telemetry (category, id);

-- ---------------------------------------------------------------------------
-- PASSO 2 de 3
-- (category, received_at): mesma consulta quando a ordem é cronológica, que é
-- o caminho da carga inicial do painel.
-- ---------------------------------------------------------------------------
create index concurrently if not exists telemetry_category_received_idx
    on public.telemetry (category, received_at);

-- ---------------------------------------------------------------------------
-- PASSO 3 de 3
-- Atualiza as estatísticas para o planejador enxergar os índices novos. Sem
-- isto o Postgres pode continuar ignorando o que acabou de ser criado.
-- (Este roda em transação sem problema; é `concurrently` que não pode.)
-- ---------------------------------------------------------------------------
analyze public.telemetry;

-- ---------------------------------------------------------------------------
-- CONFERIR (opcional, roda em bloco único)
-- Um índice que falhou no meio fica como INVÁLIDO e não é usado pelo
-- planejador, embora apareça na lista. `indisvalid` é o que denuncia:
--
--     select i.relname as indice, x.indisvalid as valido
--       from pg_class t
--       join pg_index x on x.indrelid = t.oid
--       join pg_class i on i.oid = x.indexrelid
--      where t.relname = 'telemetry'
--        and i.relname like 'telemetry_category%';
--
-- Se algum vier com valido = false, apague-o (`drop index <nome>;`) e repita o
-- passo correspondente — reexecutar por cima não conserta, porque o
-- `if not exists` vê o índice inválido e não faz nada.
