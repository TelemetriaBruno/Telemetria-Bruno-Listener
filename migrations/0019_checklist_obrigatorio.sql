-- ============================================================================
-- Migration 0019: checklist OBRIGATORIO (da equipe) x checklist DO CLIENTE.
--
-- O cliente passa a criar os proprios checklists, que valem so para as
-- maquinas dele. O que a equipe Bruno cria e obrigatorio: o cliente ve, mas
-- nao altera, nao desativa e nao apaga.
--
--   mandatory   true  = criado pela equipe (tenant_id NULL vale para todos os
--                       clientes; preenchido, para um cliente so)
--               false = criado pelo cliente; tenant_id e sempre o dele
--
-- tenant_id sozinho nao separa os dois casos: a equipe tambem cria checklist
-- para um cliente especifico. Os modelos que ja existem foram todos criados
-- pela equipe, por isso o DEFAULT true.
--
-- Aplicar ANTES de subir o backend desta versao: ele grava a coluna nova.
-- ============================================================================

ALTER TABLE checklist_templates
    ADD COLUMN IF NOT EXISTS mandatory BOOLEAN NOT NULL DEFAULT true;

-- O PostgREST guarda o esquema em cache: sem isto ele recusa a coluna nova
-- ate ser reiniciado.
NOTIFY pgrst, 'reload schema';
