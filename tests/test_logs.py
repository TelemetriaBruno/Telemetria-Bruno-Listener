"""
Logs do Listener (24/11/2026: a linha por mensagem encheu o disco do servidor).

Rodar a partir da raiz do Listener:  python -m unittest discover -s tests -t .
"""

import io
import time
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone

from app.application.ingest_telemetry import IngestTelemetry
from app.domain.entities import Machine
from app.infrastructure.ingest_pipeline import IngestPipeline
from app.infrastructure.ingest_resumo import MAX_PRIMEIRAS, ResumoIngestao
from app.infrastructure.logfmt import LogLimitado, descrever_erro, formatar, log, url_segura
from app.infrastructure.supabase import _resposta
from tests.captura_log import Logs

T0 = datetime(2026, 11, 25, 14, 0, 0, tzinfo=timezone.utc)


class Relogio:
    """Relogio monotonic falso: so anda quando o teste manda."""

    def __init__(self, agora=1000.0):
        self.agora = agora

    def __call__(self):
        return self.agora


def espera(cond, prazo=5.0):
    fim = time.monotonic() + prazo
    while time.monotonic() < fim:
        if cond():
            return True
        time.sleep(0.01)
    return False


class TestFormatador(unittest.TestCase):
    def test_comeca_com_nivel_e_evt_e_segue_a_ordem_dos_campos(self):
        self.assertEqual(
            formatar("INFO", "mqtt.conectado", host="broker.exemplo.com", porta=8883),
            "nivel=INFO evt=mqtt.conectado host=broker.exemplo.com porta=8883",
        )

    def test_espaco_igual_e_aspas_vao_entre_aspas(self):
        self.assertEqual(formatar("INFO", "x", maquina="TRHD BF6969"), 'nivel=INFO evt=x maquina="TRHD BF6969"')
        self.assertEqual(formatar("INFO", "x", v="a=b"), 'nivel=INFO evt=x v="a=b"')
        self.assertEqual(formatar("INFO", "x", v='diz "oi"'), r'nivel=INFO evt=x v="diz \"oi\""')

    def test_barra_invertida_e_escapada_dentro_das_aspas(self):
        self.assertEqual(formatar("INFO", "x", v='C:\\a b'), r'nivel=INFO evt=x v="C:\\a b"')
        self.assertEqual(formatar("INFO", "x", v='"\\'), r'nivel=INFO evt=x v="\"\\"')
        # sem nada que exija aspas, fica como esta
        self.assertEqual(formatar("INFO", "x", v="a\\b"), "nivel=INFO evt=x v=a\\b")

    def test_vazio_vai_entre_aspas_e_none_some(self):
        self.assertEqual(formatar("INFO", "x", v=""), 'nivel=INFO evt=x v=""')
        self.assertEqual(formatar("WARN", "x", status=None, erro="E: f"), 'nivel=WARN evt=x erro="E: f"')

    def test_numeros_e_bool(self):
        self.assertEqual(
            formatar("INFO", "x", a=0, b=-12, c=1.5, d=10.0, e=0.1234567, f=True, g=False),
            "nivel=INFO evt=x a=0 b=-12 c=1.5 d=10 e=0.123 f=true g=false",
        )

    def test_quebra_de_linha_vira_espaco(self):
        self.assertEqual(
            formatar("ERRO", "x", topico="THOR - Serie BF0304\r/Dados1", erro="linha 1\r\nlinha 2\nfim\t."),
            'nivel=ERRO evt=x topico="THOR - Serie BF0304 /Dados1" erro="linha 1 linha 2 fim ."',
        )

    def test_datetime_em_iso_utc(self):
        brt = timezone(timedelta(hours=-3))
        quando = datetime(2026, 11, 25, 11, 3, 7, 999000, tzinfo=brt)
        self.assertEqual(formatar("WARN", "x", ultima_msg=quando), "nivel=WARN evt=x ultima_msg=2026-11-25T14:03:07Z")

    def test_excecao_vira_tipo_e_mensagem_cortada_em_300(self):
        self.assertEqual(formatar("ERRO", "x", erro=RuntimeError("conexao caiu")), 'nivel=ERRO evt=x erro="RuntimeError: conexao caiu"')
        self.assertEqual(formatar("ERRO", "x", erro=TimeoutError()), "nivel=ERRO evt=x erro=TimeoutError")
        linha = formatar("ERRO", "x", erro=ValueError("y" * 1000))
        valor = linha.split("erro=", 1)[1]
        self.assertEqual(len(valor), 300 + 2)  # 300 caracteres + as aspas
        self.assertTrue(valor.endswith('..."'))

    def test_resposta_cortada_em_200(self):
        linha = formatar("ERRO", "x", resposta="z" * 500)
        self.assertEqual(len(linha.split("resposta=", 1)[1]), 200)

    def test_chave_em_minusculas_ascii(self):
        self.assertEqual(formatar("info", "x", Maquina="A", situação="b"), "nivel=INFO evt=x maquina=A situa__o=b")

    def test_erro_do_requests_perde_a_querystring_e_a_senha_da_url(self):
        exc = ConnectionError(
            "HTTPSConnectionPool(host='rds.exemplo.com', port=443): Max retries exceeded with url: "
            "/rest/v1/machines?machine_code=eq.TITAN+BF0167&select=id%2Ctenant_id&limit=1 "
            "(Caused by NewConnectionError('falhou'))"
        )
        texto = descrever_erro(exc)
        self.assertNotIn("machine_code", texto)
        self.assertNotIn("select=", texto)
        self.assertIn("/rest/v1/machines?(omitida) (Caused by", texto)
        self.assertNotIn("segredo", descrever_erro(ValueError("https://user:segredo@host/x falhou")))
        self.assertIn("(read timeout=10)", descrever_erro(TimeoutError("Read timed out. (read timeout=10)")))

    def test_url_segura(self):
        self.assertEqual(url_segura("https://u:senha@rds.exemplo.com:8443/rest?apikey=x#f"), "https://rds.exemplo.com:8443/rest")
        self.assertEqual(url_segura(""), "")

    def test_log_escreve_uma_linha_no_stdout(self):
        saida = io.StringIO()
        with redirect_stdout(saida):
            log("INFO", "fila.em_dia")
            log("WARN", "fila.atrasada", pendentes=250)
        self.assertEqual(saida.getvalue(), "nivel=INFO evt=fila.em_dia\nnivel=WARN evt=fila.atrasada pendentes=250\n")


class FakeResp:
    def __init__(self, texto, corpo=None):
        self.text = texto
        self._corpo = corpo

    def json(self):
        if self._corpo is None:
            raise ValueError("nao e json")
        return self._corpo


class TestRespostaDoBanco(unittest.TestCase):
    def test_linha_recusada_nao_volta_para_o_log(self):
        corpo = {
            "code": "23502",
            "details": 'Failing row contains (1, t-1, m-1, Dados1, {"Tcc": "Ligado"}).',
            "hint": None,
            "message": 'null value in column "machine_id" violates not-null constraint',
        }
        texto = _resposta(FakeResp("ignorado", corpo))
        self.assertEqual(texto, '23502 | null value in column "machine_id" violates not-null constraint')
        self.assertNotIn("Ligado", _resposta(FakeResp('erro: Failing row contains ({"Tcc": "Ligado"})')))
        self.assertEqual(len(_resposta(FakeResp("x" * 900))), 200)


class TestLogLimitado(unittest.TestCase):
    def test_repeticao_vira_suprimidas_na_proxima_que_passa(self):
        logs, relogio = Logs(), Relogio()
        limitado = LogLimitado(logs, intervalo_s=60, clock=relogio)
        for _ in range(5):
            limitado("ERRO", "banco.insert_falhou", tabela="telemetry", status=401, tentativa=1)
        limitado("ERRO", "banco.insert_falhou", tabela="telemetry", status=500, tentativa=1)  # outra falha
        relogio.agora += 60
        limitado("ERRO", "banco.insert_falhou", tabela="telemetry", status=401, tentativa=2)
        self.assertEqual(logs.linhas, [
            "nivel=ERRO evt=banco.insert_falhou tabela=telemetry status=401 tentativa=1",
            "nivel=ERRO evt=banco.insert_falhou tabela=telemetry status=500 tentativa=1",
            "nivel=ERRO evt=banco.insert_falhou tabela=telemetry status=401 tentativa=2 suprimidas=4",
        ])

    def test_tipo_da_excecao_faz_parte_da_chave_e_a_memoria_e_limitada(self):
        logs, relogio = Logs(), Relogio()
        limitado = LogLimitado(logs, clock=relogio, max_chaves=3)
        limitado("WARN", "posicao.falha", erro=KeyError("Lat"))
        limitado("WARN", "posicao.falha", erro=KeyError("Long"))
        limitado("WARN", "posicao.falha", erro=TypeError("x"))
        self.assertEqual(len(logs.eventos), 2)
        for i in range(10):
            limitado("WARN", "banco.erro_rede", tabela=f"t{i}")
        self.assertLessEqual(len(limitado._vistas), 3)


def contar(resumo, maquina, n, resultado="gravada", categoria="Dados1 Maquina"):
    for _ in range(n):
        resumo.gravou(maquina, categoria, resultado)


class TestResumoPorMaquina(unittest.TestCase):
    def test_uma_linha_por_maquina_por_janela_com_as_contagens(self):
        logs, relogio = Logs(), Relogio()
        resumo = ResumoIngestao(logs, relogio)
        contar(resumo, "TRHD BF6969", 20, "gravada", "Dados1 Maquina")
        contar(resumo, "TRHD BF6969", 278, "mantida", "Dados3 Maquina")
        contar(resumo, "TRHD BF6969", 2, "FALHOU", "Alarmes")
        contar(resumo, "TITAN BF0167", 3)
        relogio.agora += 59
        resumo.tick()
        self.assertEqual(logs.eventos, [], "a janela ainda nao fechou")
        relogio.agora += 1
        resumo.tick()
        # em ordem alfabetica de maquina
        self.assertEqual(logs.linhas, [
            'nivel=INFO evt=mqtt.resumo maquina="TITAN BF0167" janela_s=60 msgs=3 gravadas=3 mantidas=0 falhas=0 topicos=1',
            'nivel=WARN evt=mqtt.resumo maquina="TRHD BF6969" janela_s=60 msgs=300 gravadas=20 mantidas=278 falhas=2 topicos=3',
        ])
        # janela seguinte: so quem teve atividade nela
        contar(resumo, "TITAN BF0167", 1, "mantida")
        relogio.agora += 30
        resumo.tick()
        self.assertEqual(len(logs.eventos), 2)
        relogio.agora += 30
        resumo.tick()
        self.assertEqual(logs.linhas[2:], [
            'nivel=INFO evt=mqtt.resumo maquina="TITAN BF0167" janela_s=60 msgs=1 gravadas=0 mantidas=1 falhas=0 topicos=1',
        ])

    def test_janela_sem_atividade_nao_escreve_nada(self):
        logs, relogio = Logs(), Relogio()
        resumo = ResumoIngestao(logs, relogio)
        for _ in range(5):
            relogio.agora += 60
            resumo.tick()
        resumo.fechar()
        self.assertEqual(logs.eventos, [])

    def test_encerramento_escreve_a_janela_em_curso(self):
        logs, relogio = Logs(), Relogio()
        resumo = ResumoIngestao(logs, relogio)
        contar(resumo, "TITAN BF0167", 7)
        relogio.agora += 23
        resumo.fechar()
        self.assertEqual(logs.linhas, [
            'nivel=INFO evt=mqtt.resumo maquina="TITAN BF0167" janela_s=23 msgs=7 gravadas=7 mantidas=0 falhas=0 topicos=1',
        ])


class TestDescartes(unittest.TestCase):
    def test_topico_invalido_primeira_por_hora_e_resumo_global(self):
        logs, relogio = Logs(), Relogio()
        resumo = ResumoIngestao(logs, relogio)
        for _ in range(3):
            resumo.topico_invalido("/Dados1 Maquina")
        resumo.topico_invalido("BF6969")
        self.assertEqual(logs.linhas, [
            'nivel=WARN evt=mqtt.topico_invalido topico="/Dados1 Maquina"',
            "nivel=WARN evt=mqtt.topico_invalido topico=BF6969",
        ])
        relogio.agora += 60
        resumo.tick()
        self.assertEqual(logs.linhas[2:], [
            "nivel=WARN evt=mqtt.resumo_descartes janela_s=60 topico_invalido=4 maquina_nao_resolvida=0 falha_preparar=0",
        ])
        # dentro da hora o mesmo topico so conta; passada a hora, aparece de novo
        relogio.agora += 1800
        resumo.topico_invalido("/Dados1 Maquina")
        self.assertEqual(len(logs.linhas_de("mqtt.topico_invalido")), 2)
        relogio.agora += 1800
        resumo.topico_invalido("/Dados1 Maquina")
        self.assertEqual(len(logs.linhas_de("mqtt.topico_invalido")), 3)

    def test_maquina_nao_resolvida_e_erro(self):
        logs, relogio = Logs(), Relogio()
        resumo = ResumoIngestao(logs, relogio)
        for _ in range(10):
            resumo.maquina_nao_resolvida("TITAN BF0999", "TITAN BF0999/Dados1 Maquina")
        relogio.agora += 60
        resumo.tick()
        self.assertEqual(logs.linhas, [
            'nivel=ERRO evt=mqtt.maquina_nao_resolvida maquina="TITAN BF0999" topico="TITAN BF0999/Dados1 Maquina"',
            "nivel=ERRO evt=mqtt.resumo_descartes janela_s=60 topico_invalido=0 maquina_nao_resolvida=10 falha_preparar=0",
        ])

    def test_memoria_dos_topicos_invalidos_e_limitada(self):
        logs, relogio = Logs(), Relogio()
        resumo = ResumoIngestao(logs, relogio)
        for i in range(MAX_PRIMEIRAS + 50):
            resumo.topico_invalido(f"lixo{i}")
        self.assertEqual(len(logs.linhas_de("mqtt.topico_invalido")), MAX_PRIMEIRAS)
        relogio.agora += 60
        resumo.tick()
        [(_, d)] = logs.de("mqtt.resumo_descartes")
        self.assertEqual(d["topico_invalido"], MAX_PRIMEIRAS + 50)  # todos contados


class TestSilencio(unittest.TestCase):
    def test_sem_dados_uma_vez_e_voltou(self):
        logs, relogio = Logs(), Relogio()
        resumo = ResumoIngestao(logs, relogio, janela_s=10_000)  # so o silencio aqui
        resumo.chegou("TRHD BF6969", T0)
        relogio.agora += 120
        resumo.tick()
        self.assertEqual(logs.eventos, [], "120 s ainda nao e silencio")
        relogio.agora += 1
        resumo.tick()
        relogio.agora += 300
        resumo.tick()  # continua em silencio: nao repete
        self.assertEqual(logs.linhas, [
            'nivel=WARN evt=maquina.sem_dados maquina="TRHD BF6969" ultima_msg=2026-11-25T14:00:00Z silencio_s=121',
        ])
        resumo.chegou("TRHD BF6969", T0 + timedelta(seconds=421))
        self.assertEqual(logs.linhas[1:], ['nivel=INFO evt=maquina.voltou maquina="TRHD BF6969" parada_s=421'])
        # mandando de novo: nada ate o proximo silencio
        relogio.agora += 60
        resumo.chegou("TRHD BF6969", T0 + timedelta(seconds=481))
        relogio.agora += 100
        resumo.tick()
        self.assertEqual(len(logs.eventos), 2)

    def test_so_a_maquina_calada_e_avisada(self):
        logs, relogio = Logs(), Relogio()
        resumo = ResumoIngestao(logs, relogio, janela_s=10_000)
        resumo.chegou("A", T0)
        resumo.chegou("B", T0)
        for s in range(1, 200):
            relogio.agora += 1
            resumo.chegou("B", T0 + timedelta(seconds=s))
            resumo.tick()
        self.assertEqual([c["maquina"] for _, c in logs.de("maquina.sem_dados")], ["A"])


class TestEsteiraComRelogioFalso(unittest.TestCase):
    """A esteira de verdade, com o relogio falso: o silencio e detectado pela
    manutencao periodica, sem depender de mensagem chegar."""

    def test_silencio_retorno_e_resumo_pela_esteira(self):
        maquina = Machine(id="m-1", machine_code="TRHD BF6969", tenant_id="t-1")

        class Maquinas:
            def ensure_by_code(self, code):
                return maquina

        class Banco:
            def __init__(self):
                self.gravadas = []

            def save(self, event):
                self.gravadas.append(event)
                return True

        banco, logs, relogio = Banco(), Logs(), Relogio()
        p = IngestPipeline(IngestTelemetry(machines=Maquinas(), telemetry=banco), log=logs, clock=relogio)
        p.start()
        p.submit("TRHD BF6969/Dados1 Maquina", b'{"Tcc": "Ligado"}', 1, False, received_at=T0)
        self.assertTrue(espera(lambda: len(banco.gravadas) == 1))
        relogio.agora += 121  # dois minutos e um segundo sem mensagem
        self.assertTrue(espera(lambda: logs.de("maquina.sem_dados")), logs.linhas)
        self.assertEqual(logs.linhas_de("maquina.sem_dados"), [
            'nivel=WARN evt=maquina.sem_dados maquina="TRHD BF6969" ultima_msg=2026-11-25T14:00:00Z silencio_s=121',
        ])
        self.assertEqual(logs.linhas_de("mqtt.resumo"), [
            'nivel=INFO evt=mqtt.resumo maquina="TRHD BF6969" janela_s=121 msgs=1 gravadas=1 mantidas=0 falhas=0 topicos=1',
        ])
        p.submit("TRHD BF6969/Dados1 Maquina", b'{"Tcc": "Desligado"}', 1, False)
        self.assertTrue(espera(lambda: logs.de("maquina.voltou")))
        self.assertTrue(p.stop(timeout=5.0))
        self.assertEqual(logs.linhas_de("maquina.voltou"), ['nivel=INFO evt=maquina.voltou maquina="TRHD BF6969" parada_s=121'])
        self.assertEqual(len(logs.linhas_de("mqtt.resumo")), 2)  # a segunda fechada no encerramento


if __name__ == "__main__":
    unittest.main()
