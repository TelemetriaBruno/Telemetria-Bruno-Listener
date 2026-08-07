-- ============================================================================
-- Migration 0006: OPERADORES (responsavel por maquina).
--
-- O cliente cadastra as pessoas que operam as maquinas dele e aponta quem
-- responde por cada uma. E um cadastro do cliente, nao uma conta de acesso:
-- operador NAO faz login, nao tem senha e nao existe no Supabase Auth. Serve
-- para o gestor saber a quem recorrer quando uma maquina alarma ou para.
--
-- Modelo:
--   operators              pessoas do cliente (nome, documento, telefone)
--   machines.operator_id   responsavel ATUAL da maquina (no maximo um)
--
-- Uma maquina tem no maximo um responsavel; um operador pode responder por
-- varias maquinas. Por isso o vinculo mora em machines (FK simples) e nao em
-- tabela de ligacao: N:N permitiria dois responsaveis pela mesma maquina, que
-- e exatamente o que este cadastro existe para evitar.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- OPERATORS (pessoas de um cliente)
--   - tenant_id NOT NULL: operador solto nao existe, ele e sempre de um cliente
--     (e o escopo por tenant e o que impede um cliente de ver os do outro).
--   - ON DELETE CASCADE: apagado o cliente, some o cadastro de gente dele.
--   - document: matricula/CPF, opcional. UNIQUE POR CLIENTE quando preenchido:
--     dois clientes diferentes podem ter a mesma matricula, o mesmo cliente nao.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS operators (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   UUID        NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    name        TEXT        NOT NULL,
    document    TEXT,
    phone       TEXT,
    -- inativo = saiu da empresa/mudou de funcao. Nao apagamos o registro para
    -- nao perder o rastro de quem respondia pela maquina no passado.
    status      TEXT        NOT NULL DEFAULT 'active'
                            CHECK (status IN ('active', 'inactive')),
    notes       TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_operators_tenant ON operators (tenant_id);

-- Documento unico dentro do cliente. Indice parcial: documento em branco/nulo
-- e o caso comum (campo opcional) e nao deve colidir com outro em branco.
CREATE UNIQUE INDEX IF NOT EXISTS idx_operators_tenant_document
    ON operators (tenant_id, document)
    WHERE document IS NOT NULL AND document <> '';

-- ---------------------------------------------------------------------------
-- MACHINES.operator_id (responsavel atual)
--   ON DELETE SET NULL: apagado o operador, a maquina fica SEM responsavel em
--   vez de bloquear a exclusao ou sumir junto.
-- ---------------------------------------------------------------------------
ALTER TABLE machines
    ADD COLUMN IF NOT EXISTS operator_id UUID
    REFERENCES operators (id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_machines_operator ON machines (operator_id);

-- ---------------------------------------------------------------------------
-- ROW LEVEL SECURITY
--   Mesma postura das demais tabelas (0001): a service_role usada pelo backend
--   ignora o RLS, e sem policies ninguem com chave de usuario acessa.
-- ---------------------------------------------------------------------------
ALTER TABLE operators ENABLE ROW LEVEL SECURITY;
