-- Índices da tabela `telemetry` para a tela de Histórico (TimeMachine).
--
-- PROBLEMA
-- Toda consulta de histórico filtra por máquina E por janela de tempo:
--
--     where machine_id = :m and received_at between :a and :b
--     order by id
--
-- Sem um índice que cubra as duas colunas, o Postgres tem de escolher entre
-- percorrer o índice de received_at (e descartar as linhas das outras
-- máquinas) ou varrer a tabela. Com centenas de milhares de linhas isso passa
-- do statement timeout do Supabase e a consulta é cancelada com o erro 57014
-- ("canceling statement due to statement timeout") — a tela mostra "erro ao
-- carregar a série" ou fica minutos girando.
--
-- Medição que motivou este arquivo (máquina com 606 mil linhas):
--     só machine_id, sem janela ......................  0,43 s
--     só janela, sem machine_id ......................  5,05 s
--     machine_id + janela (o que a tela faz) ......... falha em 8,5 s
--
-- Cada filtro isolado funciona; a combinação é que não tem índice.

-- COMO APLICAR (leia antes de colar)
--
-- Este arquivo NÃO roda de uma vez. `CREATE INDEX CONCURRENTLY` não pode rodar
-- dentro de uma transação, e o SQL Editor do Supabase envolve em UMA transação
-- tudo o que está na aba — não um statement por vez. Colar o arquivo inteiro
-- devolve:
--
--     ERROR: 25001: CREATE INDEX CONCURRENTLY cannot run inside a transaction block
--
-- Rode um PASSO por vez, deixando na aba SOMENTE aquele comando (apague o resto
-- ou selecione a linha e use "Run selected").

-- ---------------------------------------------------------------------------
-- PASSO 1 de 4
-- Índice principal: resolve o filtro composto e já entrega as linhas na ordem
-- cronológica que a paginação do histórico consome.
-- ---------------------------------------------------------------------------
create index concurrently if not exists telemetry_machine_received_idx
    on public.telemetry (machine_id, received_at);

-- ---------------------------------------------------------------------------
-- PASSO 2 de 4
-- O seletor de "Dado" e a série de um campo específico filtram também por
-- data_type (`category`). Este índice atende essas consultas sem forçar o
-- planejador a filtrar linhas de outros data_types depois de lê-las.
-- ---------------------------------------------------------------------------
create index concurrently if not exists telemetry_machine_category_received_idx
    on public.telemetry (machine_id, category, received_at);

-- ---------------------------------------------------------------------------
-- PASSO 3 de 4
-- A paginação por cursor ordena por id dentro da janela; ter id no índice
-- evita um passo de ordenação em cima do resultado filtrado.
-- ---------------------------------------------------------------------------
create index concurrently if not exists telemetry_machine_id_idx
    on public.telemetry (machine_id, id);

-- ---------------------------------------------------------------------------
-- PASSO 4 de 4
-- Atualiza as estatísticas para o planejador enxergar os índices novos.
-- ---------------------------------------------------------------------------
analyze public.telemetry;

-- ---------------------------------------------------------------------------
-- CONFERIR (opcional, roda em bloco único)
-- Um índice interrompido no meio fica INVÁLIDO e o planejador o ignora, embora
-- ele apareça na lista. `indisvalid` denuncia:
--
--     select i.relname as indice, x.indisvalid as valido
--       from pg_class t
--       join pg_index x on x.indrelid = t.oid
--       join pg_class i on i.oid = x.indexrelid
--      where t.relname = 'telemetry'
--        and i.relname like 'telemetry_machine%';
--
-- Se algum vier com valido = false, apague-o (`drop index <nome>;`) e repita o
-- passo — reexecutar por cima não conserta, porque o `if not exists` vê o
-- índice inválido e não faz nada.
