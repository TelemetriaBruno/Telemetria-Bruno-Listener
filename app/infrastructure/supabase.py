"""
Adaptadores Supabase (API REST / PostgREST): implementam as portas
MachineRepository e TelemetryRepository usando HTTP.
"""

import time

import requests

from app.domain.entities import Machine, TelemetryEvent
from app.infrastructure.config import Settings


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
            print("[REST] POSTGREST_URL/POSTGREST_KEY ou SUPABASE_URL/SUPABASE_KEY nao configurados no .env")
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
                print(
                    f"[REST] Falha ao consultar machines: HTTP {resp.status_code} {resp.text[:200]}"
                )
                return None
            rows = resp.json()
        except requests.RequestException as exc:
            print(f"[REST] Erro de rede consultando machines: {exc}")
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
                print(
                    f"[REST] Falha ao auto-registrar maquina: HTTP {resp.status_code} {resp.text[:200]}"
                )
                # Pode ter sido criada em paralelo; tenta ler de novo.
                return self.find_by_code(machine_code)
            rows = resp.json()
        except requests.RequestException as exc:
            print(f"[REST] Erro de rede auto-registrando maquina: {exc}")
            return None

        if not rows:
            return self.find_by_code(machine_code)

        row = rows[0]
        print(f"[NOVA] Maquina auto-registrada (pendente): {machine_code!r}")
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
                print(f"[REST] insert telemetry HTTP {resp.status_code}: {resp.text[:300]}")
                if 400 <= resp.status_code < 500:
                    return None
            except requests.RequestException as exc:
                print(f"[REST] Erro de rede no insert (tentativa {attempt}): {exc}")
            time.sleep(1.5 * attempt)
        print("[REST] Telemetria NAO gravada apos retries.")
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
            print(f"[ESTADO] Nao deu para conferir as colunas do estado mantido: {exc}")
            return False
        if resp.status_code != 200:
            print(f"[ESTADO] Banco sem as colunas do estado mantido (HTTP {resp.status_code}): {resp.text[:200]}")
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
            print(f"[ESTADO] Nao deu para conferir a permissao de atualizar a telemetria: {exc}")
            return False
        if resp.status_code in (200, 204):
            return True
        print(f"[ESTADO] Sem permissao para atualizar held_until/repeats (HTTP {resp.status_code}): {resp.text[:200]}")
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
            print(f"[ESTADO] Erro de rede ao estender a linha {row_id}: {exc}")
            return False
        if resp.status_code in (200, 204):
            return True
        print(f"[ESTADO] Atualizar a linha {row_id}: HTTP {resp.status_code}: {resp.text[:200]}")
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
                print(f"[REST] insert alarm HTTP {resp.status_code}: {resp.text[:200]}")
                if 400 <= resp.status_code < 500:
                    return False
            except requests.RequestException as exc:
                print(f"[REST] Erro de rede no insert de alarme (tentativa {attempt}): {exc}")
            time.sleep(1.0 * attempt)
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
            print(f"[REST] consulta de alarme recente HTTP {resp.status_code}: {resp.text[:200]}")
        except (requests.RequestException, ValueError) as exc:
            print(f"[REST] Erro ao consultar alarme recente (grava assim mesmo): {exc}")
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
                print(f"[REST] resolve alarm HTTP {resp.status_code}: {resp.text[:200]}")
                if 400 <= resp.status_code < 500:
                    return False
            except requests.RequestException as exc:
                print(f"[REST] Erro de rede ao resolver alarmes (tentativa {attempt}): {exc}")
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
                print(f"[REST] Falha ao ler metadata da maquina: HTTP {resp.status_code}")
                return False
            rows = resp.json()
        except requests.RequestException as exc:
            print(f"[REST] Erro de rede lendo metadata: {exc}")
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
            print(f"[REST] update posicao HTTP {resp.status_code}: {resp.text[:200]}")
        except requests.RequestException as exc:
            print(f"[REST] Erro de rede gravando posicao: {exc}")
        return False
