-- ============================================================================
-- Migration 0025: VARIAS cameras por maquina.
--
-- A 0015 deu a cada maquina UMA camera (camera_path). A TITAN BF0167 tem duas
-- (192.168.82.10 e .11 atras da mesma RUTX), e o painel passa a mostrar uma
-- por vez, com botoes para alternar.
--
--   cameras  lista JSON, na ordem do painel:
--            [{"path": "cam-titan-bf0167", "label": "Frontal"}, ...]
--            path  = caminho no servidor de video (MediaMTX)
--            label = nome no painel; nulo = o painel numera ("Câmera 2")
--
-- Coluna em machines, e nao tabela nova: o backend troca a lista inteira num
-- PATCH so (nunca fica pela metade), e o papel do PostgREST ja grava machines,
-- sem GRANT novo. Nao e uma chave em machines.metadata pelo mesmo motivo da
-- 0015: o Listener regrava o metadata inteiro a cada posicao de GPS.
--
-- camera_path continua, com a PRIMEIRA camera da lista (o backend grava as
-- duas colunas juntas). E o que o backend anterior le, entao voltar a versao
-- dele nao some com a camera. O UNIQUE dela barra a primeira camera em duas
-- maquinas; as demais quem barra e o backend, que confere todas antes de
-- gravar (so admin grava).
--
-- Traz junto o conteudo da 0015, idempotente: ela consta como pendente, e
-- assim esta roda certo com ou sem ela aplicada.
--
-- Ordem: esta migration ANTES do backend novo. O backend novo sem ela ainda
-- mostra a camera de camera_path, mas o cadastro responde 503 ("migration
-- pendente"). O backend anterior com ela continua igual.
-- ============================================================================

BEGIN;

-- 0015, caso nunca tenha rodado
ALTER TABLE machines
    ADD COLUMN IF NOT EXISTS camera_path TEXT
    CHECK (camera_path ~ '^[a-z0-9][a-z0-9-]{0,39}$');

CREATE UNIQUE INDEX IF NOT EXISTS idx_machines_camera_path
    ON machines (camera_path)
    WHERE camera_path IS NOT NULL;

-- 0025
ALTER TABLE machines
    ADD COLUMN IF NOT EXISTS cameras JSONB NOT NULL DEFAULT '[]'::jsonb
    CHECK (jsonb_typeof(cameras) = 'array');

-- Quem ja tinha camera cadastrada passa a te-la como primeira da lista.
UPDATE machines
   SET cameras = jsonb_build_array(jsonb_build_object('path', camera_path, 'label', NULL))
 WHERE camera_path IS NOT NULL
   AND cameras = '[]'::jsonb;

COMMIT;

-- O PostgREST guarda o esquema em cache: sem isto ele recusa gravar a coluna
-- nova (PGRST204) ate ser reiniciado.
NOTIFY pgrst, 'reload schema';

-- Conferencia (so leitura):
--   SELECT machine_code, camera_path, cameras FROM machines
--    WHERE camera_path IS NOT NULL OR cameras <> '[]'::jsonb;
