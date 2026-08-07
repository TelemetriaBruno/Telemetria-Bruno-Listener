-- Coluna `locked` de `login_attempts`: a trava permanente do freio de login.
--
-- PROBLEMA
-- O código grava e lê `locked` (LoginAttempt.locked, SupabaseLoginAttemptRepository,
-- ListLockedAccounts), mas a coluna nunca foi criada no banco. O PostgREST
-- recusa a escrita inteira:
--
--     PGRST204: Could not find the 'locked' column of 'login_attempts'
--               in the schema cache
--     42703:    column login_attempts.locked does not exist
--
-- Não é cache desatualizado: o 42703 vem do próprio Postgres.
--
-- EFEITO ENQUANTO NÃO EXISTE
-- `register_failure` monta a linha com `locked` e faz upsert. O POST falha
-- por inteiro, então NADA é gravado — nem `fails`, nem `blocks`. Ou seja: o
-- freio de força bruta não conta tentativa nenhuma e o login fica sem
-- proteção, além de devolver 502 a quem erra a senha.
--
-- SEMÂNTICA
-- false = sujeita apenas à escada progressiva de bloqueios temporários.
-- true  = travada de vez (3o bloqueio do mesmo e-mail); só a equipe libera,
--         via POST /api/admin/locked-accounts/unlock. Nenhuma espera destrava.

alter table public.login_attempts
    add column if not exists locked boolean not null default false;

-- A tela de Acessos lista as travadas ordenadas pela última atualização.
-- Índice parcial: só as travadas interessam, e elas são poucas.
create index if not exists login_attempts_locked_idx
    on public.login_attempts (updated_at desc)
    where locked;

-- O PostgREST mantém um cache do schema; sem isto ele pode seguir devolvendo
-- PGRST204 mesmo com a coluna já criada.
notify pgrst, 'reload schema';
