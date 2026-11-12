"""
Testes da esteira de ingestao (incidente de 12/11/2026: banco lento travava a
thread do MQTT e o painel mostrava todas as maquinas offline).

Rodar a partir da raiz do Listener:  python -m unittest discover -s tests -t .
"""

import threading
import time
import unittest
from datetime import datetime, timedelta, timezone

from app.application.ingest_telemetry import IngestStatus, IngestTelemetry
from app.domain.entities import Machine
from app.infrastructure.ingest_pipeline import IngestPipeline
from app.infrastructure.machine_cache import STALE_RETRY_S, CachedMachineRepository

MAQUINA = Machine(id="m-1", machine_code="TITAN BF0167", tenant_id="t-1")


class Maquinas:
    def __init__(self, maquina=MAQUINA):
        self.maquina = maquina
        self.chamadas = 0

    def find_by_code(self, code):
        return self.ensure_by_code(code)

    def ensure_by_code(self, code):
        self.chamadas += 1
        return self.maquina if code == MAQUINA.machine_code else None


class BancoLento:
    """Telemetria que demora `atraso` segundos por gravacao, como o RDS nas quedas."""

    def __init__(self, atraso=0.0, falha=False):
        self.atraso = atraso
        self.falha = falha
        self.gravadas = []
        self.lock = threading.Lock()

    def save(self, event):
        time.sleep(self.atraso)
        with self.lock:
            self.gravadas.append(event)
        return not self.falha


class Painel:
    def __init__(self):
        self.eventos = []
        self.instantes = []

    def notify(self, event):
        self.eventos.append(event)
        self.instantes.append(time.monotonic())


class Reserva:
    def __init__(self):
        self.escritas = []

    def write(self, event):
        self.escritas.append(event)


def esteira(banco, painel=None, reserva=None, **kw):
    ingest = IngestTelemetry(machines=Maquinas(), telemetry=banco)
    logs = []
    p = IngestPipeline(ingest, notifier=painel, overflow=reserva, log=logs.append, **kw)
    p.start()
    return p, logs


def espera(cond, prazo=5.0):
    fim = time.monotonic() + prazo
    while time.monotonic() < fim:
        if cond():
            return True
        time.sleep(0.01)
    return False


class TestEsteira(unittest.TestCase):
    def test_banco_lento_nao_trava_a_recepcao_nem_o_painel(self):
        banco, painel = BancoLento(atraso=2.0), Painel()
        p, _ = esteira(banco, painel)
        inicio = time.monotonic()
        for i in range(5):
            p.submit("TITAN BF0167/EstadoAlarmes", b'{"n": %d}' % i, 1, False)
        self.assertLess(time.monotonic() - inicio, 0.05, "submit nao pode esperar o banco")
        # o painel recebe as 5 leituras sem esperar nenhuma gravacao de 2 s
        self.assertTrue(espera(lambda: len(painel.eventos) == 5, prazo=1.0))
        self.assertLess(painel.instantes[-1] - inicio, 1.0)
        self.assertLessEqual(len(banco.gravadas), 1)
        p.stop(timeout=0.1)

    def test_ordem_e_hora_da_chegada_preservadas(self):
        banco, painel = BancoLento(atraso=0.01), Painel()
        p, logs = esteira(banco, painel)
        chegada = datetime(2026, 11, 12, 17, 43, 18, tzinfo=timezone.utc)
        for i in range(20):
            p.submit("TITAN BF0167/Dados1 Maquina", b'{"i": %d}' % i, 1, False,
                     received_at=chegada + timedelta(seconds=i))
        self.assertTrue(p.stop(timeout=5.0))
        self.assertEqual([e.payload["i"] for e in banco.gravadas], list(range(20)))
        self.assertEqual(banco.gravadas[0].received_at, chegada)
        self.assertEqual(banco.gravadas[19].received_at, chegada + timedelta(seconds=19))
        self.assertEqual([e.payload["i"] for e in painel.eventos], list(range(20)))
        self.assertEqual(sum("[MSG] TITAN BF0167/Dados1 Maquina (tenant=t-1) gravada" in l for l in logs), 20)

    def test_hora_e_carimbada_na_chegada_e_nao_na_gravacao(self):
        banco = BancoLento(atraso=0.3)
        p, _ = esteira(banco)
        antes = datetime.now(timezone.utc)
        p.submit("TITAN BF0167/A", b"{}", 1, False)
        p.submit("TITAN BF0167/B", b"{}", 1, False)
        self.assertTrue(p.stop(timeout=5.0))
        # a segunda foi gravada ~0,6 s depois, mas guarda a hora em que chegou
        self.assertLess(banco.gravadas[1].received_at - antes, timedelta(seconds=0.1))

    def test_fila_de_gravacao_cheia_manda_a_mais_antiga_para_a_reserva(self):
        liberar = threading.Event()

        class BancoParado(BancoLento):
            def save(self, event):
                liberar.wait(5)
                return super().save(event)

        banco, reserva = BancoParado(), Reserva()
        p, logs = esteira(banco, reserva=reserva, max_gravacao=3)
        for i in range(10):
            p.submit("TITAN BF0167/X", b'{"i": %d}' % i, 1, False)
        self.assertTrue(espera(lambda: len(reserva.escritas) >= 6))
        liberar.set()
        self.assertTrue(p.stop(timeout=5.0))
        guardadas = sorted(e.payload["i"] for e in banco.gravadas + reserva.escritas)
        self.assertEqual(guardadas, list(range(10)), "nenhuma leitura pode sumir")
        self.assertEqual(p.descartadas, len(reserva.escritas))
        self.assertTrue(any("[FILA] Fila cheia" in l for l in logs))

    def test_topico_invalido_e_maquina_sem_cadastro_nao_param_a_esteira(self):
        banco, painel = BancoLento(), Painel()
        ingest = IngestTelemetry(machines=Maquinas(maquina=None), telemetry=banco)
        logs = []
        p = IngestPipeline(ingest, notifier=painel, log=logs.append)
        p.start()
        p.submit("sem-tipo", b"{}", 1, False)
        p.submit("TITAN BF0167/Y", b"{}", 1, False)
        self.assertTrue(p.stop(timeout=5.0))
        self.assertEqual(banco.gravadas, [])
        self.assertEqual(painel.eventos, [])
        self.assertTrue(any(l.startswith("[SKIP]") for l in logs))
        self.assertTrue(any("Nao foi possivel resolver" in l for l in logs))

    def test_excecao_na_gravacao_nao_mata_a_thread(self):
        class BancoQuebrado(BancoLento):
            def save(self, event):
                if event.payload.get("i") == 0:
                    raise RuntimeError("conexao caiu")
                return super().save(event)

        banco = BancoQuebrado()
        p, logs = esteira(banco)
        p.submit("TITAN BF0167/Z", b'{"i": 0}', 1, False)
        p.submit("TITAN BF0167/Z", b'{"i": 1}', 1, False)
        self.assertTrue(p.stop(timeout=5.0))
        self.assertEqual([e.payload["i"] for e in banco.gravadas], [1])
        self.assertTrue(any("FALHOU" in l for l in logs))

    def test_aviso_quando_a_gravacao_acumula(self):
        liberar = threading.Event()

        class BancoParado(BancoLento):
            def save(self, event):
                liberar.wait(5)
                return True

        p, logs = esteira(BancoParado())
        for i in range(260):
            p.submit("TITAN BF0167/W", b"{}", 1, False)
        self.assertTrue(espera(lambda: p.pendentes >= 250))
        liberar.set()
        self.assertTrue(espera(lambda: any("Gravacao em dia" in l for l in logs)))
        self.assertTrue(p.stop(timeout=5.0))
        self.assertTrue(any("aguardando gravacao" in l for l in logs))


class TestExecuteContinuaIgual(unittest.TestCase):
    def test_execute_grava_e_respeita_received_at(self):
        banco = BancoLento()
        ingest = IngestTelemetry(machines=Maquinas(), telemetry=banco)
        quando = datetime(2026, 11, 12, 18, 17, tzinfo=timezone.utc)
        r = ingest.execute("TITAN BF0167/H", b'{"h": 1}', 1, False, received_at=quando)
        self.assertIs(r.status, IngestStatus.SAVED)
        self.assertEqual(banco.gravadas[0].received_at, quando)
        r = IngestTelemetry(machines=Maquinas(), telemetry=BancoLento(falha=True)).execute(
            "TITAN BF0167/H", b"{}", 1, False
        )
        self.assertIs(r.status, IngestStatus.SAVE_FAILED)

    def test_prepare_nao_grava(self):
        banco = BancoLento()
        r = IngestTelemetry(machines=Maquinas(), telemetry=banco).prepare("TITAN BF0167/H", b"{}", 1, False)
        self.assertIs(r.status, IngestStatus.PREPARED)
        self.assertEqual(banco.gravadas, [])


class TestCacheDeMaquinas(unittest.TestCase):
    def test_banco_fora_usa_o_cadastro_antigo_e_espera_para_tentar_de_novo(self):
        inner = Maquinas()
        cache = CachedMachineRepository(inner, ttl_seconds=300)
        agora = [1000.0]
        import app.infrastructure.machine_cache as mc
        original = mc.time.monotonic
        mc.time.monotonic = lambda: agora[0]
        try:
            self.assertEqual(cache.ensure_by_code("TITAN BF0167"), MAQUINA)
            agora[0] += 301  # venceu o TTL
            inner.maquina = None  # e o banco nao responde
            self.assertEqual(cache.ensure_by_code("TITAN BF0167"), MAQUINA)
            chamadas = inner.chamadas
            agora[0] += STALE_RETRY_S - 1  # dentro do prazo: nem tenta o banco
            self.assertEqual(cache.ensure_by_code("TITAN BF0167"), MAQUINA)
            self.assertEqual(inner.chamadas, chamadas)
            agora[0] += 2  # passou o prazo: tenta de novo
            inner.maquina = MAQUINA
            self.assertEqual(cache.ensure_by_code("TITAN BF0167"), MAQUINA)
            self.assertEqual(inner.chamadas, chamadas + 1)
        finally:
            mc.time.monotonic = original

    def test_maquina_nunca_vista_com_banco_fora_continua_none(self):
        cache = CachedMachineRepository(Maquinas(maquina=None), ttl_seconds=300)
        self.assertIsNone(cache.ensure_by_code("TITAN BF0167"))


if __name__ == "__main__":
    unittest.main()
