-- ---------------------------------------------------------------------------
-- 0005 — Tentativas de login (freio de forca bruta)
--
-- POST /api/auth/login aceitava disparos sucessivos sem limite proprio. O
-- GoTrue tem throttle, mas nao e nosso nem configuravel: nao sabemos o limiar
-- e nao controlamos a politica.
--
-- Esta tabela e o estado do freio. Uma linha por CHAVE, onde chave e o e-mail
-- tentado OU o IP de origem (contadores independentes: o que estourar primeiro
-- bloqueia). Cobre os dois padroes de ataque — alguem martelando UMA conta, e
-- uma origem varrendo MUITAS contas com senha comum.
--
-- Fica no Supabase (e nao em memoria) para sobreviver a restart e valer para
-- todos os workers: com o estado no processo, N workers multiplicam o limite
-- por N e um deploy zera o bloqueio de quem estava sendo atacado.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS login_attempts (
    -- 'email:cliente@acme.com' ou 'ip:203.0.113.7'. O prefixo mantem os dois
    -- espacos de nomes separados numa tabela so (um IP nunca colide com um
    -- e-mail).
    key            TEXT PRIMARY KEY,
    -- falhas consecutivas desde o ultimo sucesso/expiracao
    fails          INTEGER     NOT NULL DEFAULT 0,
    -- quantos bloqueios ja foram aplicados a esta chave: e o indice da escada
    -- progressiva (1min, 5min, 15min, 30min, 60min)
    blocks         INTEGER     NOT NULL DEFAULT 0,
    -- ate quando esta bloqueada (NULL = livre). Passado = bloqueio expirado.
    blocked_until  TIMESTAMPTZ,
    -- Bloqueio PERMANENTE da conta, aplicado no 3o bloqueio: a escada temporaria
    -- nao segurou o ataque, entao a conta so volta pela mao da equipe (a tela de
    -- Acessos destrava). Vale apenas para chaves 'email:' — um IP nunca e
    -- travado para sempre, porque IP compartilhado (escritorio atras de NAT)
    -- derrubaria varios usuarios legitimos de uma vez.
    locked         BOOLEAN     NOT NULL DEFAULT FALSE,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Varredura da limpeza periodica (linhas antigas e ja liberadas).
CREATE INDEX IF NOT EXISTS idx_login_attempts_updated ON login_attempts (updated_at);

-- ---------------------------------------------------------------------------
-- ROW LEVEL SECURITY
--   Mesma postura das demais tabelas: a secret key (service_role) que o backend
--   usa IGNORA o RLS, entao a leitura/escrita do freio continua funcionando.
--   Sem policies, nenhuma chave de usuario (anon/authenticated) le esta tabela
--   — o que importa aqui, porque ela revela QUAIS e-mails existem e estao sob
--   ataque.
-- ---------------------------------------------------------------------------
ALTER TABLE login_attempts ENABLE ROW LEVEL SECURITY;
