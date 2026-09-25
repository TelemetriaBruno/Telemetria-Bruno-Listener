-- ============================================================================
-- Migration 0015: CAMERA da maquina.
--
-- camera_path e o nome do caminho no servidor de video (MediaMTX) que exibe a
-- camera desta maquina, ex.: 'cam-bf0304'. NULL = maquina sem camera.
--
-- Coluna propria, e nao uma chave em machines.metadata: o Listener regrava o
-- metadata inteiro a cada posicao de GPS (le, funde, grava). Uma edicao da
-- camera no meio desse ciclo seria sobrescrita em silencio.
--
-- UNIQUE parcial: dois cadastros apontando para o mesmo caminho mostrariam a
-- camera de uma maquina na tela de outra, possivelmente de outro cliente.
-- ============================================================================

ALTER TABLE machines
    ADD COLUMN IF NOT EXISTS camera_path TEXT
    CHECK (camera_path ~ '^[a-z0-9][a-z0-9-]{0,39}$');

CREATE UNIQUE INDEX IF NOT EXISTS idx_machines_camera_path
    ON machines (camera_path)
    WHERE camera_path IS NOT NULL;
