-- ============================================================================
-- Migration 0018: RESOLUCAO de alarmes.
--
-- O firmware real (TITAN BF0167) manda um alarme por mensagem nos topicos
-- "Alarmes" (CLP) e "Alarmes Motor Diesel" (central do motor), e quando nao ha
-- mais alarme manda, no MESMO topico, o texto "Maquina sem alarmes". Ate aqui
-- a tabela so guardava o disparo, entao o painel nao tinha como dizer quais
-- alarmes estao ativos agora nem quando cada um se resolveu.
--
--   category    topico de origem (o data_type). O "sem alarmes" de um topico
--               fecha so os alarmes dele: o do CLP nao resolve os do motor.
--   cleared_at  hora do "Maquina sem alarmes" que fechou o alarme (hora da
--               maquina quando conhecida). NULL = alarme ainda ativo.
--
-- Linhas anteriores ficam com category NULL; o Listener as fecha com o "sem
-- alarmes" de qualquer topico, ja que nao da para saber de qual vieram.
--
-- UNIQUE parcial: o mesmo alarme repetido enquanto ainda esta aberto e o mesmo
-- disparo, nao um novo. O Listener tenta inserir e trata o conflito como "ja
-- aberto", entao o historico nao enche de copias nem a aba de ativos mostra o
-- mesmo alarme duas vezes.
--
-- Aplicar ANTES de subir o Listener e o backend desta versao: os dois leem e
-- gravam as colunas novas.
-- ============================================================================

ALTER TABLE alarms ADD COLUMN IF NOT EXISTS category   TEXT;
ALTER TABLE alarms ADD COLUMN IF NOT EXISTS cleared_at TIMESTAMPTZ;

-- Consulta da aba Alarmes: os ativos de UMA maquina.
CREATE INDEX IF NOT EXISTS idx_alarms_open
    ON alarms (machine_id)
    WHERE cleared_at IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ux_alarms_open_identity
    ON alarms (machine_id, COALESCE(category, ''), COALESCE(code, description, ''))
    WHERE cleared_at IS NULL;

-- O PostgREST guarda o esquema em cache: sem isto ele recusa as colunas novas
-- ate ser reiniciado.
NOTIFY pgrst, 'reload schema';
