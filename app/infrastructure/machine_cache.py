"""
Decorator de cache com TTL sobre qualquer MachineRepository.

Tambem memoriza resultado negativo (codigo conhecido-como-inexistente) para
nao re-consultar em rajada uma maquina nao cadastrada — mas respeitando o TTL,
para que uma maquina cadastrada DEPOIS seja reconhecida sem reiniciar o servico.
"""

import time

from app.application.ports import MachineRepository
from app.domain.entities import Machine


# Com o banco fora, quanto esperar para tentar renovar o cadastro de novo.
STALE_RETRY_S = 30.0


class CachedMachineRepository:
    def __init__(self, inner: MachineRepository, ttl_seconds: float):
        self._inner = inner
        self._ttl = ttl_seconds
        # machine_code -> {"machine": Machine | None, "ts": float}
        self._cache: dict[str, dict] = {}

    def find_by_code(self, machine_code: str) -> Machine | None:
        now = time.monotonic()
        cached = self._cache.get(machine_code)
        if cached is not None and (now - cached["ts"]) < self._ttl:
            return cached["machine"]

        machine = self._inner.find_by_code(machine_code)
        # So memoriza o negativo se veio de consulta valida ("nao existe");
        # falha de rede/config tambem retorna None, mas o inner ja logou e
        # queremos re-tentar na proxima mensagem. Nao ha como distinguir aqui,
        # entao memorizamos com TTL do mesmo jeito — comportamento identico
        # ao subscriber original.
        self._cache[machine_code] = {"machine": machine, "ts": now}
        return machine

    def ensure_by_code(self, machine_code: str) -> Machine | None:
        now = time.monotonic()
        cached = self._cache.get(machine_code)
        if cached is not None and cached["machine"] is not None:
            if (now - cached["ts"]) < self._ttl:
                return cached["machine"]

        machine = self._inner.ensure_by_code(machine_code)
        # So cacheia resultado positivo; se deu None (falha), nao memoriza para
        # tentar de novo na proxima mensagem.
        if machine is not None:
            self._cache[machine_code] = {"machine": machine, "ts": now}
            return machine
        # Falhou a renovacao de uma maquina que ja conheciamos (banco lento ou
        # fora): segue com o cadastro antigo em vez de perder a leitura, e so
        # tenta renovar de novo daqui a STALE_RETRY_S. Sem esse prazo, cada
        # mensagem esperaria o timeout do banco e travaria o envio ao painel.
        if cached is not None and cached["machine"] is not None:
            self._cache[machine_code] = {
                "machine": cached["machine"],
                "ts": now - self._ttl + STALE_RETRY_S,
            }
            return cached["machine"]
        return None
