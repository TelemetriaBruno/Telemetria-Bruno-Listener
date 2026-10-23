"""
Reprocessa as linhas de alarme ja gravadas em telemetry com as regras atuais do
Listener (app.domain.payload e app.domain.alarm).

Por que existe: ate out/2026 o Listener so reconhecia "Alarmes Ativos" (nome do
simulador) e so lia UTF-8. A maquina real publica em "Alarmes" e "Alarmes Motor
Diesel", com texto em Windows-1252, entao:
  - a tabela `alarms` ficou vazia para ela;
  - toda mensagem com acento foi gravada como {"raw_hex": ..., "encoding": "binary"}.

Para cada linha das categories de alarme, a ferramenta:
  1. redecodifica o payload que ficou em raw_hex e regrava a linha em telemetry;
  2. insere a ativacao em `alarms` (source_id = id da linha), pulando o que ja
     existe (pelo source_id ou por maquina + instante + codigo + descricao, que
     e o que o Listener grava sem source_id). Pode ser rodada de novo sem duplicar.
  3. no "Maquina sem alarmes", fecha os alarmes abertos daquele topico, como o
     Listener faz ao vivo (as linhas vem em ordem de id, a de chegada).

Uso (na pasta do Listener, com o mesmo .env do servico):
    python -m tools.reprocessar_alarmes             # so mostra o que faria
    python -m tools.reprocessar_alarmes --aplicar   # grava
    python -m tools.reprocessar_alarmes --desde 2026-10-23T09:10:00-03:00
        # so as mensagens que chegaram a partir dai (ex.: recuperar os disparos
        # que a regra da 0018 recusou, sem trazer de volta o que foi apagado)
"""

import argparse
from datetime import datetime, timezone

from app.domain.alarm import alarm_from_payload, is_all_clear, message_time
from app.domain.payload import decode_payload
from app.infrastructure.config import Settings
from app.infrastructure.supabase import SupabaseAlarmRepository, build_http_session

CATEGORIES = ("Alarmes", "Alarmes Motor Diesel", "Alarmes Ativos")
PAGE = 1000


def _utc(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def _key(machine_id, activated_at: datetime, code, description):
    return (machine_id, activated_at.astimezone(timezone.utc), code, description)


def _get(http, base, table, params, timeout):
    resp = http.get(f"{base}/{table}", params=params, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--aplicar", action="store_true", help="grava (sem isto, so simula)")
    ap.add_argument("--desde", type=datetime.fromisoformat, default=None,
                    help="so mensagens recebidas a partir deste instante ISO-8601 (com fuso)")
    ap.add_argument("categories", nargs="*", default=CATEGORIES)
    args = ap.parse_args()

    settings = Settings.from_env()
    base, timeout = settings.rest_url, max(settings.http_timeout, 30)
    if not base or not settings.rest_key:
        raise SystemExit("POSTGREST_URL/POSTGREST_KEY nao configurados no .env")
    http = build_http_session(settings)
    alarms = SupabaseAlarmRepository(settings, http)

    # tabela pequena (evento raro): cabe inteira em memoria para o dedup
    existing = _get(http, base, "alarms", {"select": "machine_id,code,description,activated_at,source_id",
                                          "limit": "100000"}, timeout)
    by_source = {a["source_id"] for a in existing if a["source_id"] is not None}
    by_content = {_key(a["machine_id"], _utc(a["activated_at"]), a["code"], a["description"]) for a in existing}
    print(f"alarms hoje: {len(existing)} linhas")
    print("MODO:", "APLICANDO" if args.aplicar else "SIMULACAO (use --aplicar para gravar)")
    if args.desde is not None:
        if args.desde.tzinfo is None:
            raise SystemExit("--desde precisa de fuso (ex.: 2026-10-23T09:10:00-03:00)")
        print(f"DESDE: {args.desde.isoformat()}")

    for category in args.categories:
        n = fixed = new = dup = fail = 0
        cursor = 0
        while True:
            params = {
                "select": "id,tenant_id,machine_id,received_at,payload",
                "category": f"eq.{category}", "id": f"gt.{cursor}",
                "order": "id.asc", "limit": str(PAGE)}
            if args.desde is not None:
                params["received_at"] = f"gte.{args.desde.isoformat()}"
            rows = _get(http, base, "telemetry", params, timeout)
            if not rows:
                break
            cursor = rows[-1]["id"]
            for row in rows:
                n += 1
                payload = row["payload"] or {}
                if "raw_hex" in payload:
                    redecoded = decode_payload(bytes.fromhex(payload["raw_hex"]), category)
                    if "raw_hex" not in redecoded:
                        payload = redecoded
                        fixed += 1
                        if args.aplicar:
                            resp = http.patch(f"{base}/telemetry", params={"id": f"eq.{row['id']}"},
                                              json={"payload": payload}, headers={"Prefer": "return=minimal"},
                                              timeout=timeout)
                            if resp.status_code not in (200, 204):
                                print(f"  [ERRO] telemetry {row['id']}: HTTP {resp.status_code} {resp.text[:200]}")
                                fail += 1

                if is_all_clear(payload):
                    when = message_time(payload, _utc(row["received_at"]))
                    print(f"  = {when:%d/%m %H:%M:%S}  sem alarmes")
                    if args.aplicar and not alarms.clear(row["machine_id"], category, when):
                        fail += 1
                    continue
                alarm = alarm_from_payload(payload, _utc(row["received_at"]))
                if alarm is None:
                    continue
                key = _key(row["machine_id"], alarm.activated_at, alarm.code, alarm.description)
                if row["id"] in by_source or key in by_content:
                    dup += 1
                    continue
                new += 1
                by_content.add(key)
                print(f"  + {alarm.activated_at:%d/%m %H:%M:%S}  {alarm.code} - {alarm.description}")
                if args.aplicar and not alarms.save(
                    tenant_id=row["tenant_id"], machine_id=row["machine_id"], code=alarm.code,
                    description=alarm.description, activated_at=alarm.activated_at, source_id=row["id"],
                    category=category,
                ):
                    fail += 1
        print(f"=== {category}: {n} linhas | payload redecodificado: {fixed} | "
              f"ativacoes novas: {new} | ja existiam: {dup} | falhas: {fail}")


if __name__ == "__main__":
    main()
