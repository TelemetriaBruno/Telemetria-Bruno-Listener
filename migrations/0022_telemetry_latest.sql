-- telemetry_latest: a última leitura de CADA data_type de cada máquina.
--
-- PROBLEMA
-- O painel montava o estado atual da máquina com as últimas 1.000 linhas dela,
-- mais uma lista fixa de tópicos "esparsos". Com o motor parado, a TITAN BF0167
-- deixa de publicar "Dados1 Motor Diesel", "Dados1 Maquina", "Dados Alimentacao" e
-- "Pressao Bombas", e continua com "EstadoAlarmes" (~1,2 s). Em ~20 min esses
-- tópicos saíam da janela, e o card de RPM SUMIA da tela numa máquina que já tinha
-- rodado, até o motor ligar de novo.
--
-- Medido em 05/11/2026: 10.053 linhas durante uma parada de 4 h do motor, e 3.318
-- linhas depois do último quadro dele no momento da medição.
--
-- Regra do usuário: campo que já foi apresentado nunca deixa de ser exibido, mesmo
-- que o tópico não esteja mais sendo enviado.
--
-- SOLUÇÃO
-- Perguntar ao banco, em vez de torcer para a leitura caber numa janela. Uma lista
-- fixa de tópicos não serve: o firmware pode mandar um tópico novo a qualquer hora.
--
-- CUSTO
-- `select distinct on` varreria todas as linhas da máquina (21 mil por dia). A CTE
-- recursiva salta de categoria em categoria pelo índice
-- telemetry_machine_category_received_idx (machine_id, category, received_at), da
-- migration 0007: duas buscas no índice por tópico, seja qual for o tamanho da
-- tabela. A mesma consulta levou 20 ms no RDS, com 437 mil linhas e 14 tópicos.
--
-- Não confere dono: quem chama (GET /api/telemetry/latest) só passa as máquinas que
-- o usuário pode ver, a mesma regra de GET /api/machines/{id}/telemetry.
--
-- Roda em transação, sem passos: é só uma função, não toca em nenhuma linha.

create or replace function public.telemetry_latest(p_machine_ids uuid[])
returns setof public.telemetry
language sql
stable
as $$
    with recursive categorias(machine_id, category) as (
        select
            m.id,
            (
                select t.category
                from public.telemetry t
                where t.machine_id = m.id and t.category is not null
                order by t.category
                limit 1
            )
        from unnest(p_machine_ids) as m(id)

        union all

        select
            c.machine_id,
            (
                select t.category
                from public.telemetry t
                where t.machine_id = c.machine_id and t.category > c.category
                order by t.category
                limit 1
            )
        from categorias c
        where c.category is not null
    )
    select l.*
    from categorias c
    cross join lateral (
        select t.*
        from public.telemetry t
        where t.machine_id = c.machine_id and t.category = c.category
        order by t.received_at desc
        limit 1
    ) l
    where c.category is not null;
$$;

-- Para todos, e não para os papéis do Supabase (anon, authenticated...), que podem
-- não existir no RDS. A função roda com os direitos de quem chama: sem SELECT na
-- telemetry, ela não entrega nada.
grant execute on function public.telemetry_latest(uuid[]) to public;

-- O PostgREST guarda as funções em cache: sem isto a rota responde PGRST202 até ele
-- reiniciar.
notify pgrst, 'reload schema';

-- ROLLBACK
--   drop function if exists public.telemetry_latest(uuid[]);
