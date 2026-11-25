"""
Adaptadores Supabase (API REST / PostgREST): implementam as portas
MachineRepository e TelemetryRepository usando HTTP.

Log (logfmt.py): erros com `tabela`, `operacao`, `status` e no maximo 200
caracteres do corpo da resposta em `resposta=` (nunca o payload da telemetria
nem a querystring). Os que podem se repetir a cada mensagem passam pelo
`_log_limitado`: uma linha por minuto por tipo de falha, com `suprimidas=N`.
"""

import time

import requests

from app.domain.entities import Machine, TelemetryEvent
from app.infrastructure import logfmt
from app.infrastructure.config import Settings

_log = logfmt.log
_log_limitado = logfmt.LogLimitado()


def _resposta(resp: requests.Response) -> str:
    """O essencial do corpo de erro do PostgREST, em ate 200 caracteres.

    O "Failing row contains (...)" que vem em `details` (violacao de NOT NULL,
    CHECK) repete a linha inteira, payload incluido: fica de fora."""
    try:
        corpo = resp.json()
    except Exception:  # corpo que nao e JSON (HTML do proxy, texto): usa o texto
        corpo = None
    if isinstance(corpo, dict) and ("message" in corpo or "code" in corpo):
        partes = [
            str(corpo[k])
            for k in ("code", "message", "details", "hint")
            if corpo.get(k) and not str(corpo[k]).startswith("Failing row contains")
        ]
        texto = " | ".join(partes)
    else:
        texto = resp.text or ""
        corte = texto.find("Failing row contains")
        if corte >= 0:
            texto = texto[:corte] + "Failing row contains (omitida)"
    return texto[:200]


def _nivel_http(status: int) -> str:
    """4xx nao tem nova tentativa (a leitura se perde): ERRO. 5xx tenta de novo: WARN."""
    return "ERRO" if 400 <= status < 500 else "WARN"


def build_http_session(settings: Settings) -> requests.Session:
    """Sessao HTTP reutilizada (keep-alive) com as credenciais do Supabase."""
    session = requests.Session()
    session.headers.update(
        {
            "apikey": settings.rest_key or "",
            "Authorization": f"Bearer {settings.rest_key}" if settings.rest_key else "",
            "Content-Type": "application/json",
        }
    )
    return session


class SupabaseMachineRepository:
    def __init__(self, settings: Settings, http: requests.Session):
        self._rest_base = settings.rest_url
        self._configured = bool(self._rest_base and settings.rest_key)
        self._timeout = settings.http_timeout
        self._http = http

    def find_by_code(self, machine_code: str) -> Machine | None:
        if not self._configured:
            # POSTGREST_URL/POSTGREST_KEY ou SUPABASE_URL/SUPABASE_KEY vazios no .env
            _log_limitado("ERRO", "banco.sem_configuracao", tabela="machines")
            return None

        try:
            resp = self._http.get(
                f"{self._rest_base}/machines",
                params={
                    "machine_code": f"eq.{machine_code}",
                    "select": "id,tenant_id,status",
                    "limit": "1",
                },
                timeout=self._timeout,
            )
            if resp.status_code != 200:
                # WARN: maquina ja conhecida segue com o cadastro em cache; a leitura
                # perdida (maquina nova) sai como ERRO em mqtt.maquina_nao_resolvida
                _log_limitado(
                    "WARN", "banco.consulta_falhou", tabela="machines", maquina=machine_code,
                    status=resp.status_code, resposta=_resposta(resp),
                )
                return None
            rows = resp.json()
        except requests.RequestException as exc:
            _log_limitado(
                "WARN", "banco.erro_rede", tabela="machines", operacao="consulta", maquina=machine_code, erro=exc
            )
            return None

        if not rows:
            return None

        row = rows[0]
        return Machine(
            id=row["id"],
            machine_code=machine_code,
            tenant_id=row.get("tenant_id"),
        )

    def ensure_by_code(self, machine_code: str) -> Machine | None:
        """Retorna a maquina; se nao existir, auto-registra como pendente."""
        existing = self.find_by_code(machine_code)
        if existing is not None:
            return existing

        if not self._configured:
            return None

        # Auto-registro: cria a maquina 'pendente' (sem cliente). O upsert com
        # merge-duplicates trata corrida entre mensagens da mesma maquina nova.
        body = {"machine_code": machine_code, "status": "unassigned"}
        try:
            resp = self._http.post(
                f"{self._rest_base}/machines",
                params={"on_conflict": "machine_code"},
                json=body,
                headers={
                    "Prefer": "resolution=merge-duplicates,return=representation",
                },
                timeout=self._timeout,
            )
            if resp.status_code not in (200, 201):
                _log_limitado(
                    "WARN", "banco.insert_falhou", tabela="machines", maquina=machine_code,
                    status=resp.status_code, resposta=_resposta(resp), tentativa=1,
                )
                # Pode ter sido criada em paralelo; tenta ler de novo.
                return self.find_by_code(machine_code)
            rows = resp.json()
        except requests.RequestException as exc:
            _log_limitado(
                "WARN", "banco.insert_falhou", tabela="machines", maquina=machine_code, erro=exc, tentativa=1
            )
            return None

        if not rows:
            return self.find_by_code(machine_code)

        row = rows[0]
        # auto-registrada como pendente (sem cliente)
        _log("INFO", "maquina.registrada", maquina=machine_code)
        return Machine(id=row["id"], machine_code=machine_code, tenant_id=row.get("tenant_id"))


class SupabaseTelemetryRepository:
    def __init__(self, settings: Settings, http: requests.Session):
        self._rest_base = settings.rest_url
        self._timeout = settings.http_timeout
        self._http = http

    def save(self, event: TelemetryEvent) -> bool:
        return self._insert(event, want_id=False) is not None

    def insert(self, event: TelemetryEvent) -> int | None:
        """Grava e devolve o id da linha (0 = gravou mas a resposta nao trouxe o
        id; None = nao gravou). E o id que o estado mantido atualiza depois."""
        return self._insert(event, want_id=True)

    def _insert(self, event: TelemetryEvent, want_id: bool) -> int | None:
        body = {
            "tenant_id": event.tenant_id,
            "machine_id": event.machine_id,
            "category": event.category,
            "topic": event.topic,
            "qos": event.qos,
            "retain": event.retain,
            "received_at": event.received_at.isoformat(),
            "payload": event.payload,
        }
        for attempt in (1, 2, 3):
            try:
                resp = self._http.post(
                    f"{self._rest_base}/telemetry",
                    params={"select": "id"} if want_id else None,
                    json=body,
                    headers={"Prefer": "return=representation" if want_id else "return=minimal"},
                    timeout=self._timeout,
                )
                if resp.status_code in (200, 201, 204):
                    if not want_id:
                        return 0
                    try:
                        rows = resp.json()
                        return int(rows[0]["id"]) if rows else 0
                    except (ValueError, KeyError, IndexError, TypeError):
                        return 0
                _log_limitado(
                    _nivel_http(resp.status_code), "banco.insert_falhou", tabela="telemetry",
                    status=resp.status_code, resposta=_resposta(resp), tentativa=attempt,
                )
                if 400 <= resp.status_code < 500:
                    return None
            except requests.RequestException as exc:
                _log_limitado("WARN", "banco.insert_falhou", tabela="telemetry", erro=exc, tentativa=attempt)
            time.sleep(1.5 * attempt)
        # a leitura nao foi gravada (conta como falha no mqtt.resumo)
        _log_limitado("ERRO", "banco.insert_desistiu", tabela="telemetry", tentativas=3)
        return None

    def supports_hold(self) -> bool:
        """O banco tem as colunas do estado mantido (migration 0026)? Sem elas a
        ingestao grava toda leitura, como antes: nada se perde por a migration
        ainda nao ter rodado."""
        try:
            resp = self._http.get(
                f"{self._rest_base}/telemetry",
                params={"select": "id,held_until,repeats", "limit": "1"},
                timeout=self._timeout,
            )
        except requests.RequestException as exc:
            _log("WARN", "estado.verificacao_falhou", etapa="colunas", erro=exc)
            return False
        if resp.status_code != 200:
            # banco sem held_until/repeats (migration 0026)
            _log("WARN", "estado.sem_colunas", status=resp.status_code, resposta=_resposta(resp))
            return False
        # A permissao de ATUALIZAR tambem: sem ela a linha do estado nunca
        # ganharia o "mantido ate", e o historico mostraria buracos. Atualiza uma
        # linha que nao existe (id -1): so a permissao e conferida.
        try:
            resp = self._http.patch(
                f"{self._rest_base}/telemetry",
                params={"id": "eq.-1"},
                json={"repeats": 0},
                headers={"Prefer": "return=minimal"},
                timeout=self._timeout,
            )
        except requests.RequestException as exc:
            _log("WARN", "estado.verificacao_falhou", etapa="permissao", erro=exc)
            return False
        if resp.status_code in (200, 204):
            return True
        # sem permissao de atualizar held_until/repeats
        _log("WARN", "estado.sem_permissao", status=resp.status_code, resposta=_resposta(resp))
        return False

    def extend_hold(self, row_id: int, held_until, repeats: int) -> bool:
        """Uma tentativa so: quem chama tenta de novo no proximo ciclo, e a
        thread de gravacao nao pode ficar presa esperando retentativas."""
        try:
            resp = self._http.patch(
                f"{self._rest_base}/telemetry",
                params={"id": f"eq.{row_id}"},
                json={"held_until": held_until.isoformat(), "repeats": int(repeats)},
                headers={"Prefer": "return=minimal"},
                timeout=self._timeout,
            )
        except requests.RequestException as exc:
            _log_limitado(
                "WARN", "banco.erro_rede", tabela="telemetry", operacao="estender_estado", linha=row_id, erro=exc
            )
            return False
        if resp.status_code in (200, 204):
            return True
        _log_limitado(
            "WARN", "banco.update_falhou", tabela="telemetry", operacao="estender_estado", linha=row_id,
            status=resp.status_code, resposta=_resposta(resp),
        )
        return False


class SupabaseAlarmRepository:
    """Grava ativacoes de alarme na tabela dedicada `alarms`. Best-effort:
    problemas aqui nao devem impedir a gravacao normal da telemetria."""

    def __init__(self, settings: Settings, http: requests.Session):
        self._rest_base = settings.rest_url
        self._timeout = settings.http_timeout
        self._http = http

    def save(
        self,
        tenant_id: str | None,
        machine_id: str,
        code: str | None,
        description: str | None,
        activated_at,
        source_id: int | None = None,
        category: str | None = None,
    ) -> bool:
        if not self._rest_base:
            return False
        body = {
            "tenant_id": tenant_id,
            "machine_id": machine_id,
            "code": code,
            "description": description,
            "activated_at": activated_at.isoformat(),
            "source_id": source_id,
            "category": category,
        }
        for attempt in (1, 2):
            try:
                resp = self._http.post(
                    f"{self._rest_base}/alarms",
                    json=body,
                    headers={"Prefer": "return=minimal"},
                    timeout=self._timeout,
                )
                if resp.status_code in (200, 201, 204):
                    return True
                # a mesma mensagem gravada de novo (UNIQUE da migration 0020:
                # maquina + topico + alarme + hora): reentrega, ja esta na tabela
                if resp.status_code == 409:
                    return True
                _log_limitado(
                    _nivel_http(resp.status_code), "banco.insert_falhou", tabela="alarms",
                    status=resp.status_code, resposta=_resposta(resp), tentativa=attempt,
                )
                if 400 <= resp.status_code < 500:
                    return False
            except requests.RequestException as exc:
                _log_limitado("WARN", "banco.insert_falhou", tabela="alarms", erro=exc, tentativa=attempt)
            time.sleep(1.0 * attempt)
        # o disparo ficou so na telemetry
        _log_limitado("ERRO", "banco.insert_desistiu", tabela="alarms", tentativas=2)
        return False

    def fired_since(self, machine_id: str, category: str, code, description, since) -> bool:
        if not self._rest_base:
            return False
        params = {
            "select": "id",
            "machine_id": f"eq.{machine_id}",
            "category": f"eq.{category}",
            "activated_at": f"gte.{since.isoformat()}",
            "limit": "1",
        }
        # mesma identidade do alarme que o painel usa (alarmKey): o codigo, e a
        # descricao so quando nao ha codigo
        if code is not None:
            params["code"] = f"eq.{code}"
        else:
            params["code"] = "is.null"
            params["description"] = f"eq.{description}"
        try:
            resp = self._http.get(f"{self._rest_base}/alarms", params=params, timeout=self._timeout)
            if resp.status_code == 200:
                return len(resp.json()) > 0
            _log_limitado(
                "WARN", "banco.consulta_falhou", tabela="alarms", operacao="alarme_recente",
                status=resp.status_code, resposta=_resposta(resp),
            )
        except (requests.RequestException, ValueError) as exc:
            # sem a resposta, o alarme e gravado assim mesmo
            evt = "banco.erro_rede" if isinstance(exc, requests.RequestException) else "banco.consulta_falhou"
            _log_limitado("WARN", evt, tabela="alarms", operacao="alarme_recente", erro=exc)
        return False

    def clear(self, machine_id: str, category: str, cleared_at) -> bool:
        if not self._rest_base:
            return False
        when = cleared_at.isoformat()
        params = {
            "machine_id": f"eq.{machine_id}",
            "cleared_at": "is.null",
            # alarme que disparou DEPOIS do aviso (mensagens fora de ordem) segue
            # aberto: o "sem alarmes" nao fala dele
            "activated_at": f"lte.{when}",
            "or": f'(category.eq."{category}",category.is.null)',
        }
        for attempt in (1, 2):
            try:
                resp = self._http.patch(
                    f"{self._rest_base}/alarms",
                    params=params,
                    json={"cleared_at": when},
                    headers={"Prefer": "return=minimal"},
                    timeout=self._timeout,
                )
                if resp.status_code in (200, 204):
                    return True
                _log_limitado(
                    _nivel_http(resp.status_code), "banco.update_falhou", tabela="alarms", operacao="resolver",
                    status=resp.status_code, resposta=_resposta(resp), tentativa=attempt,
                )
                if 400 <= resp.status_code < 500:
                    return False
            except requests.RequestException as exc:
                _log_limitado(
                    "WARN", "banco.erro_rede", tabela="alarms", operacao="resolver", erro=exc, tentativa=attempt
                )
            time.sleep(1.0 * attempt)
        return False


class SupabaseMachinePositionRepository:
    """Grava a ultima posicao conhecida em machines.metadata.

    Vai em `metadata` (JSONB que ja existe no esquema) e nao em colunas novas:
    a posicao e um atributo do cadastro, nao uma dimensao consultavel, e o
    backend ja devolve metadata inteiro em MachineOut, e com isso o painel
    recebe a coordenada sem uma linha de mudanca no backend.

    O merge e feito no cliente (le metadata, funde a chave, grava de volta)
    porque o PostgREST nao aplica patch parcial dentro de um JSONB. A corrida
    aqui e inofensiva: duas posicoes da mesma maquina a 15 min de distancia
    nao competem, e o pior caso e reescrever a chave com o valor mais novo.

    Best-effort: problemas aqui nao devem impedir a gravacao da telemetria.
    """

    def __init__(self, settings: Settings, http: requests.Session):
        self._rest_base = settings.rest_url
        self._timeout = settings.http_timeout
        self._http = http

    def update_position(
        self,
        machine_id: str,
        lat: float,
        lon: float,
        measured_at,
    ) -> bool:
        if not self._rest_base:
            return False

        try:
            resp = self._http.get(
                f"{self._rest_base}/machines",
                params={"id": f"eq.{machine_id}", "select": "metadata", "limit": "1"},
                timeout=self._timeout,
            )
            if resp.status_code != 200:
                _log_limitado(
                    "WARN", "banco.consulta_falhou", tabela="machines", operacao="ler_metadata",
                    status=resp.status_code, resposta=_resposta(resp),
                )
                return False
            rows = resp.json()
        except requests.RequestException as exc:
            _log_limitado("WARN", "banco.erro_rede", tabela="machines", operacao="ler_metadata", erro=exc)
            return False

        metadata = (rows[0].get("metadata") if rows else None) or {}
        if not isinstance(metadata, dict):
            metadata = {}
        metadata["position"] = {
            "lat": lat,
            "lon": lon,
            "at": measured_at.isoformat(),
        }

        try:
            resp = self._http.patch(
                f"{self._rest_base}/machines",
                params={"id": f"eq.{machine_id}"},
                json={"metadata": metadata},
                headers={"Prefer": "return=minimal"},
                timeout=self._timeout,
            )
            if resp.status_code in (200, 204):
                return True
            _log_limitado(
                "WARN", "banco.update_falhou", tabela="machines", operacao="posicao",
                status=resp.status_code, resposta=_resposta(resp),
            )
        except requests.RequestException as exc:
            _log_limitado("WARN", "banco.erro_rede", tabela="machines", operacao="posicao", erro=exc)
        return False
