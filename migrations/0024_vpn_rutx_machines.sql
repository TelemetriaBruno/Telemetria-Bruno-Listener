-- ============================================================================
-- Migration 0024: em qual MAQUINA esta cada RUTX da VPN.
--
-- O servidor liga RUTX e maquina pelo nome do cadastro (a RUTX TITAN-BF0167 e
-- da maquina TITAN BF0167). Isso nao serve para RUTX de estoque, que vai de uma
-- maquina para outra com o mesmo nome. Agora o admin escolhe na tela VPN, e a
-- linha desta tabela vale mais que o nome.
--
--   rutx          nome do peer no WireGuard (ex.: RUTX-0001). Nao e o IP: o IP
--                 de uma RUTX revogada volta a ficar livre e levaria o vinculo
--                 para outro aparelho.
--   machine_code  codigo da maquina (machines.machine_code). NULO = a RUTX esta
--                 sem maquina de proposito e nao e mais ligada pelo nome.
--
-- Sem UNIQUE em machine_code nem FK: uma linha de RUTX revogada (que sumiu da
-- VPN) nao pode travar a maquina, e o backend so considera RUTX que existem.
-- Ele tambem recusa ligar uma maquina que ja tem RUTX.
--
-- Pode ser aplicada antes ou depois do backend desta versao: sem a tabela, a
-- tela VPN continua ligando so pelo nome e recusa gravar vinculo.
-- ============================================================================

CREATE TABLE IF NOT EXISTS vpn_rutx_machines (
    rutx         TEXT PRIMARY KEY,
    machine_code TEXT,
    updated_by   TEXT,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Mesmo acesso da tabela operators, que o backend ja le e grava (igual a 0021).
DO $$
DECLARE
    concedido RECORD;
BEGIN
    FOR concedido IN
        SELECT a.grantee::regrole::text AS papel, a.privilege_type AS privilegio
        FROM pg_class c, aclexplode(c.relacl) a
        WHERE c.oid = 'operators'::regclass
          AND a.grantee <> 0
          AND a.grantee <> c.relowner
    LOOP
        EXECUTE format('GRANT %s ON vpn_rutx_machines TO %s', concedido.privilegio, concedido.papel);
    END LOOP;

    IF (SELECT relrowsecurity FROM pg_class WHERE oid = 'operators'::regclass)
       AND NOT EXISTS (SELECT 1 FROM pg_policy WHERE polrelid = 'operators'::regclass)
    THEN
        ALTER TABLE vpn_rutx_machines ENABLE ROW LEVEL SECURITY;
    END IF;
END $$;

-- O PostgREST guarda o esquema em cache: sem isto ele nao enxerga a tabela nova
-- ate ser reiniciado.
NOTIFY pgrst, 'reload schema';
