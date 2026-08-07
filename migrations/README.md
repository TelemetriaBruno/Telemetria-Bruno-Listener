# Migrations do banco (Supabase/Postgres)

Esta pasta é o **único** lugar de migrations da plataforma. O banco é
compartilhado: o Listener escreve telemetria e alarmes, o backend lê e serve o
painel, e ambos dependem do mesmo esquema.

Já houve duas pastas concorrentes (esta e `backend/db/`), com numerações
próprias que se entrelaçavam no tempo. O custo apareceu: a tabela `telemetry`
acumulou 9 índices vindos dos dois lados, alguns redundantes entre si, e foi
preciso uma migration só para enxugá-los (`0008_telemetry_indices_enxutos.sql`).
Uma sequência única evita o problema na origem, porque a ordem de aplicação
passa a ser legível num lugar só.

## Convenção

`NNNN_descricao_curta.sql` — quatro dígitos, `snake_case`, sem acento.

O número é a **ordem de aplicação**, não a data. Ao criar uma migration, use o
próximo número livre; nunca renumere as já aplicadas, porque o número é como
alguém confere o que já rodou no banco.

Cada arquivo começa com um bloco de comentário explicando o PROBLEMA que motivou
a mudança, com a medição quando houver. É o que permite decidir, meses depois,
se um índice ainda se justifica.

## Como aplicar

Não há runner automático: as migrations são aplicadas à mão no **SQL Editor do
Supabase**, na ordem numérica.

### A armadilha do CREATE INDEX CONCURRENTLY

O SQL Editor do Supabase envolve em **uma transação tudo o que está na aba** —
não um statement por vez. Como `CREATE INDEX CONCURRENTLY` não pode rodar dentro
de transação, colar um arquivo inteiro que o contenha devolve:

```
ERROR: 25001: CREATE INDEX CONCURRENTLY cannot run inside a transaction block
```

O erro não indica problema no índice nem no SQL: é o modo de envio. Deixe na aba
**somente aquele comando** (apague o resto, ou selecione a linha e use "Run
selected") e repita para cada um. As migrations afetadas trazem os comandos
marcados como `PASSO N de M`.

`concurrently` está lá de propósito: sem ele o índice bloqueia escritas na tabela
até terminar, e o Listener pararia de gravar telemetria nesse intervalo.

Um índice interrompido no meio fica **inválido** — aparece na lista mas o
planejador o ignora, e o `if not exists` de uma nova tentativa não o corrige.
As migrações de índice trazem a consulta que checa `indisvalid` no rodapé.

## Estado

| Migration | O que faz |
|---|---|
| 0001 | esquema inicial (tenants, machines, telemetry) |
| 0002 | seed TITAN |
| 0003 | máquina de bancada |
| 0004 | tabela `alarms` |
| 0005 | tabela `login_attempts` |
| 0006 | tabela `operators` |
| 0007 | índices compostos p/ o histórico (machine_id, received_at) |
| 0008 | remove índices redundantes acumulados |
| 0009 | coluna `locked` em login_attempts |
| 0010 | índices de `category` p/ o painel ao vivo |
| 0011 | **RPC `history_series`** — agregação no banco. **PENDENTE de aplicar** |

A 0011 é a única ainda não aplicada. Enquanto ela não rodar, o backend detecta a
ausência da função e cai no caminho de varredura em Python, que produz o mesmo
resultado a um custo bem maior — o deploy do código não depende dela.
