"""
Estado mantido (pedido de 20/11/2026): leitura igual a anterior do topico nao
vira linha nova; a linha do estado guarda ate quando o valor durou.

Rodar a partir da raiz do Listener:  python -m unittest discover -s tests -t .
"""

import unittest
from datetime import datetime, timedelta, timezone

from app.application.held_states import HeldStates
from app.application.ingest_telemetry import IngestTelemetry, PersistOutcome
from app.domain.entities import Machine, TelemetryEvent
from app.domain.hold import state_signature
from app.infrastructure.ingest_pipeline import IngestPipeline
from tests.captura_log import Logs

MAQUINA = Machine(id="m-1", machine_code="TITAN BF0999", tenant_id="t-1")
BRT = timezone(timedelta(hours=-3))


class Maquinas:
    def find_by_code(self, code):
        return MAQUINA

    def ensure_by_code(self, code):
        return MAQUINA


class Banco:
    """A tabela telemetry: cada insert e uma linha; extend_hold atualiza a linha."""

    def __init__(self, falha_insert=False):
        self.linhas: dict[int, dict] = {}
        self.updates: list[tuple[int, datetime, int]] = []
        self.falha_insert = falha_insert
        self._seq = 0

    def save(self, event):
        return self.insert(event) is not None

    def insert(self, event):
        if self.falha_insert:
            return None
        self._seq += 1
        self.linhas[self._seq] = {
            "category": event.category,
            "received_at": event.received_at,
            "payload": event.payload,
            "held_until": None,
            "repeats": 0,
        }
        return self._seq

    def extend_hold(self, row_id, held_until, repeats):
        self.updates.append((row_id, held_until, repeats))
        self.linhas[row_id]["held_until"] = held_until
        self.linhas[row_id]["repeats"] = repeats
        return True

    def em_ordem(self):
        return [self.linhas[i] for i in sorted(self.linhas)]


class Posicoes:
    def __init__(self):
        self.chamadas = 0

    def update_position(self, machine_id, lat, lon, measured_at):
        self.chamadas += 1
        return True


class Relogio:
    def __init__(self, agora):
        self.agora = agora

    def __call__(self):
        return self.agora


def evento(categoria, quando, **campos):
    payload = {"_ts": quando.astimezone(BRT).isoformat(), "HoraData": quando.astimezone(BRT).strftime("%d/%m/%Y - %H:%M:%S")}
    payload.update(campos)
    payload["_raw"] = str(campos) + payload["HoraData"]
    return TelemetryEvent(
        tenant_id="t-1", machine_id="m-1", category=categoria,
        topic=f"TITAN BF0999/{categoria}", qos=1, retain=False,
        received_at=quando, payload=payload,
    )


def montar(banco=None, relogio=None, posicoes=None):
    banco = banco or Banco()
    relogio = relogio or Relogio(datetime(2026, 11, 20, 17, 0, tzinfo=timezone.utc))
    held = HeldStates(banco, flush_after=timedelta(seconds=10), log=lambda *_a, **_k: None, clock=relogio)
    ingest = IngestTelemetry(machines=Maquinas(), telemetry=banco, held=held, positions=posicoes)
    return ingest, banco, held, relogio


class EstadoMantido(unittest.TestCase):
    def test_exemplo_do_usuario_bateria_24v_por_uma_hora_e_queda_para_12v(self):
        # 14:00 local (17:00 UTC) a 24 V, uma leitura por segundo, e às 15:00 cai
        ingest, banco, _, _ = montar()
        inicio = datetime(2026, 11, 20, 17, 0, 0, tzinfo=timezone.utc)
        for s in range(3600):
            ingest.persist(evento("Dados Motor Diesel", inicio + timedelta(seconds=s), TensaoBateria=24))
        ingest.persist(evento("Dados Motor Diesel", inicio + timedelta(hours=1), TensaoBateria=12))

        linhas = banco.em_ordem()
        self.assertEqual(len(linhas), 2)  # 3601 leituras, duas linhas
        v24, v12 = linhas
        self.assertEqual(v24["payload"]["TensaoBateria"], 24)
        self.assertEqual(v24["received_at"], inicio)
        # ficou em 24 V até a última leitura de 24 V, e o banco sabe disso
        self.assertEqual(v24["held_until"], inicio + timedelta(seconds=3599))
        self.assertEqual(v24["repeats"], 3599)
        self.assertEqual(v12["payload"]["TensaoBateria"], 12)
        self.assertEqual(v12["received_at"], inicio + timedelta(hours=1))
        self.assertIsNone(v12["held_until"])

    def test_estado_nao_atravessa_a_virada_da_hora(self):
        # 24 V das 14:30 às 15:30 (local) e depois 12 V: a hora cheia corta o estado
        ingest, banco, _, _ = montar()
        inicio = datetime(2026, 11, 20, 17, 30, 0, tzinfo=timezone.utc)
        for s in range(3600):
            ingest.persist(evento("Dados Motor Diesel", inicio + timedelta(seconds=s), TensaoBateria=24))
        ingest.persist(evento("Dados Motor Diesel", inicio + timedelta(hours=1), TensaoBateria=12))

        a, b, c = banco.em_ordem()
        self.assertEqual((a["received_at"], a["held_until"], a["repeats"]),
                         (inicio, datetime(2026, 11, 20, 17, 59, 59, tzinfo=timezone.utc), 1799))
        self.assertEqual((b["received_at"], b["held_until"], b["repeats"]),
                         (datetime(2026, 11, 20, 18, 0, 0, tzinfo=timezone.utc),
                          datetime(2026, 11, 20, 18, 29, 59, tzinfo=timezone.utc), 1799))
        self.assertEqual(b["payload"]["TensaoBateria"], 24)
        self.assertEqual(c["payload"]["TensaoBateria"], 12)
        # nenhuma leitura some: 1800 + 1800 + 1
        self.assertEqual(sum(1 + l["repeats"] for l in (a, b, c)), 3601)

    def test_hora_do_aparelho_diferente_nao_faz_o_valor_ser_novo(self):
        t0 = datetime(2026, 11, 20, 17, 0, 0, tzinfo=timezone.utc)
        a = evento("Dados3 Maquina", t0, H_Motor=1234.5, EstDir="Parada")
        b = evento("Dados3 Maquina", t0 + timedelta(seconds=1), H_Motor=1234.5, EstDir="Parada")
        self.assertNotEqual(a.payload["HoraData"], b.payload["HoraData"])
        self.assertEqual(state_signature(a.payload), state_signature(b.payload))
        c = evento("Dados3 Maquina", t0, H_Motor=1234.6, EstDir="Parada")
        self.assertNotEqual(state_signature(a.payload), state_signature(c.payload))

    def test_retorno_ao_valor_anterior_e_estado_novo(self):
        # 24 → 12 → 24: são três estados, não dois
        ingest, banco, _, _ = montar()
        t0 = datetime(2026, 11, 20, 17, 0, 0, tzinfo=timezone.utc)
        for i, v in enumerate([24, 24, 12, 12, 24]):
            ingest.persist(evento("Dados Motor Diesel", t0 + timedelta(seconds=i), TensaoBateria=v))
        self.assertEqual([l["payload"]["TensaoBateria"] for l in banco.em_ordem()], [24, 12, 24])
        self.assertEqual([l["repeats"] for l in banco.em_ordem()], [1, 1, 0])

    def test_topicos_e_maquinas_tem_estados_separados(self):
        ingest, banco, _, _ = montar()
        t0 = datetime(2026, 11, 20, 17, 0, 0, tzinfo=timezone.utc)
        for i in range(3):
            ingest.persist(evento("Dados1 Maquina", t0 + timedelta(seconds=i), Tcc="Ligado"))
            ingest.persist(evento("Dados3 Maquina", t0 + timedelta(seconds=i), EstDir="Parada"))
        self.assertEqual(len(banco.linhas), 2)

    def test_alarme_e_evento_toda_mensagem_e_gravada(self):
        ingest, banco, _, _ = montar()
        t0 = datetime(2026, 11, 20, 17, 0, 0, tzinfo=timezone.utc)
        for i in range(3):
            ingest.persist(evento("Alarmes", t0 + timedelta(minutes=5 * i), **{"A-": "9-Rede CAN IHM porta 0 em Falha"}))
        self.assertEqual(len(banco.linhas), 3)

    def test_mantida_nao_regrava_posicao(self):
        posicoes = Posicoes()
        ingest, banco, _, _ = montar(posicoes=posicoes)
        t0 = datetime(2026, 11, 20, 17, 0, 0, tzinfo=timezone.utc)
        resultados = [
            ingest.persist_outcome(evento("Dados3 Maquina", t0 + timedelta(seconds=i), Lat=-27.6, Long=-48.6))
            for i in range(5)
        ]
        self.assertEqual(resultados, [PersistOutcome.SAVED] + [PersistOutcome.HELD] * 4)
        self.assertEqual(posicoes.chamadas, 1)

    def test_mantido_ate_vai_ao_banco_a_cada_10s_e_nao_a_cada_leitura(self):
        ingest, banco, _, _ = montar()
        t0 = datetime(2026, 11, 20, 17, 0, 0, tzinfo=timezone.utc)
        for s in range(31):
            ingest.persist(evento("Dados3 Maquina", t0 + timedelta(seconds=s), EstDir="Parada"))
        # 30 repetições em 30 s: três atualizações (10 s, 20 s, 30 s), não 30
        self.assertEqual([u[2] for u in banco.updates], [10, 20, 30])

    def test_maquina_que_para_no_meio_do_estado_tem_o_fim_gravado(self):
        relogio = Relogio(datetime(2026, 11, 20, 17, 0, 0, tzinfo=timezone.utc))
        ingest, banco, _, _ = montar(relogio=relogio)
        t0 = relogio.agora
        for s in range(15):
            ingest.persist(evento("Dados3 Maquina", t0 + timedelta(seconds=s), EstDir="Parada"))
        self.assertEqual(banco.linhas[1]["repeats"], 10)  # a última atualização foi aos 10 s
        relogio.agora = t0 + timedelta(seconds=40)  # nada mais chegou
        ingest.flush_held()
        self.assertEqual(banco.linhas[1]["repeats"], 14)
        self.assertEqual(banco.linhas[1]["held_until"], t0 + timedelta(seconds=14))

    def test_leitura_fora_de_ordem_nao_estende_o_estado(self):
        ingest, banco, _, _ = montar()
        t0 = datetime(2026, 11, 20, 17, 0, 10, tzinfo=timezone.utc)
        ingest.persist(evento("Dados3 Maquina", t0, EstDir="Parada"))
        ingest.persist(evento("Dados3 Maquina", t0 - timedelta(seconds=5), EstDir="Parada"))
        self.assertEqual(len(banco.linhas), 2)

    def test_insert_que_falha_nao_abre_estado(self):
        banco = Banco(falha_insert=True)
        ingest, banco, held, _ = montar(banco=banco)
        t0 = datetime(2026, 11, 20, 17, 0, 0, tzinfo=timezone.utc)
        self.assertIs(ingest.persist_outcome(evento("Dados3 Maquina", t0, EstDir="Parada")), PersistOutcome.FAILED)
        banco.falha_insert = False
        # a próxima igual tenta gravar de novo, em vez de se achar mantida
        self.assertIs(
            ingest.persist_outcome(evento("Dados3 Maquina", t0 + timedelta(seconds=1), EstDir="Parada")),
            PersistOutcome.SAVED,
        )
        self.assertEqual(held.open_count, 1)

    def test_encerrar_a_esteira_grava_o_estado_aberto(self):
        ingest, banco, _, _ = montar()
        logs = Logs()
        pipeline = IngestPipeline(ingest, log=logs)
        pipeline.start()
        for s in range(5):
            pipeline.submit(
                "TITAN BF0999/Dados3 Maquina",
                b'{"HoraData": "20/11/2026 - 14:00:0%d", "EstDir": "Parada"}' % s,
                1, False,
                received_at=datetime(2026, 11, 20, 17, 0, s, tzinfo=timezone.utc),
            )
        self.assertTrue(pipeline.stop(timeout=10))
        self.assertEqual(len(banco.linhas), 1)
        self.assertEqual(banco.linhas[1]["repeats"], 4)
        # uma gravada e quatro mantidas, contadas no resumo e nao linha a linha
        [(_, resumo)] = logs.de("mqtt.resumo")
        self.assertEqual((resumo["msgs"], resumo["gravadas"], resumo["mantidas"], resumo["falhas"]), (5, 1, 4, 0))

    def test_sem_estado_mantido_grava_toda_leitura(self):
        banco = Banco()
        ingest = IngestTelemetry(machines=Maquinas(), telemetry=banco)
        t0 = datetime(2026, 11, 20, 17, 0, 0, tzinfo=timezone.utc)
        for s in range(3):
            ingest.persist(evento("Dados3 Maquina", t0 + timedelta(seconds=s), EstDir="Parada"))
        self.assertEqual(len(banco.linhas), 3)


if __name__ == "__main__":
    unittest.main()
