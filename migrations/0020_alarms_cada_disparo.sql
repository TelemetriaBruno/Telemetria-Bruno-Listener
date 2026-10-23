-- ============================================================================
-- Migration 0020: cada mensagem de alarme e um DISPARO.
--
-- A 0018 tratava o mesmo alarme repetido enquanto aberto como "a maquina
-- repetindo o aviso" e recusava a linha (UNIQUE parcial ux_alarms_open_identity).
-- O dado real (TITAN BF0167, 21-23/10/2026) desmente isso: a maquina manda uma
-- mensagem por ACIONAMENTO (cada aperto no botao de emergencia, cada nova
-- sobrecarga da embreagem), em horarios irregulares, e nunca mandou o "Maquina
-- sem alarmes" que fecharia o alarme. Com a 0018, so o primeiro disparo de cada
-- alarme entrava na tabela; os seguintes chegavam na telemetry e sumiam do
-- Historico e da aba Alarmes (ex.: emergencia do radio controle as 14:19 de
-- 23/10, recusada porque a das 13:35 seguia "aberta").
--
-- Agora o unico duplicado e a MESMA mensagem gravada duas vezes (reentrega do
-- QoS 1, ferramenta de reprocessamento): mesma maquina, topico, alarme e hora
-- da maquina. O Listener continua tratando o 409 como "ja gravado".
--
-- A lista de ativos (painel) mostra cada alarme uma vez, com o disparo mais
-- recente; o "Maquina sem alarmes" continua fechando todos os abertos do topico.
-- ============================================================================

DROP INDEX IF EXISTS ux_alarms_open_identity;

CREATE UNIQUE INDEX IF NOT EXISTS ux_alarms_disparo
    ON alarms (machine_id, COALESCE(category, ''), COALESCE(code, description, ''), activated_at);

NOTIFY pgrst, 'reload schema';
