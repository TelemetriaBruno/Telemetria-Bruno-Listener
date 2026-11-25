-- ============================================================================
-- Migration 0021: vinculo entre o LOGIN da equipe e o PERFIL de VPN.
--
-- A tela VPN do painel precisa saber qual tunel e de quem esta logado. Isso
-- ficava em VPN_TECHNICIANS, no .env do backend: mudar exigia editar o arquivo
-- no servidor e reiniciar a API. Agora o admin define na guia Equipe de Acessos.
--
--   email    login em minusculas
--   profile  nome do peer no WireGuard (ex.: PC-Tecnico-01)
--
-- UNIQUE em profile: a escolha da maquina vale por tunel, entao duas pessoas no
-- mesmo perfil trocariam a maquina uma da outra sem perceber.
--
-- Sem FK: o login vive no provedor de identidade e o peer, no WireGuard.
--
-- Aplicar ANTES de subir o backend desta versao: ele le esta tabela para montar
-- a tela VPN.
-- ============================================================================

CREATE TABLE IF NOT EXISTS vpn_technicians (
    email   TEXT PRIMARY KEY CHECK (email = lower(email)),
    profile TEXT NOT NULL UNIQUE
);

-- Mesmo acesso da tabela operators, que o backend ja le e grava. O papel usado
-- pelo PostgREST muda de ambiente (service_role no Supabase, outro no RDS), entao
-- os privilegios sao copiados em vez de escritos aqui. Com policies em operators,
-- o RLS nao e copiado: o papel dependeria delas, e sem elas a tabela nova ficaria
-- inacessivel.
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
        EXECUTE format('GRANT %s ON vpn_technicians TO %s', concedido.privilegio, concedido.papel);
    END LOOP;

    IF (SELECT relrowsecurity FROM pg_class WHERE oid = 'operators'::regclass)
       AND NOT EXISTS (SELECT 1 FROM pg_policy WHERE polrelid = 'operators'::regclass)
    THEN
        ALTER TABLE vpn_technicians ENABLE ROW LEVEL SECURITY;
    END IF;
END $$;

-- O PostgREST guarda o esquema em cache: sem isto ele nao enxerga a tabela nova
-- ate ser reiniciado.
NOTIFY pgrst, 'reload schema';
