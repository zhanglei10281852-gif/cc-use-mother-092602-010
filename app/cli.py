from __future__ import annotations

import argparse
import json

from fastapi.testclient import TestClient

from app.database import database_path, get_connection, init_db
from app.main import app


def command_init() -> int:
    init_db()
    print(json.dumps({"database": str(database_path()), "status": "initialized"}, ensure_ascii=False))
    return 0


def command_check() -> int:
    init_db()
    connection = get_connection()
    result = {
        "database": str(database_path()),
        "integrity": connection.execute("PRAGMA integrity_check").fetchone()[0],
        "foreign_keys": connection.execute("PRAGMA foreign_keys").fetchone()[0],
        "journal_mode": connection.execute("PRAGMA journal_mode").fetchone()[0],
        "tables": connection.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'").fetchone()[0],
    }
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result["integrity"] == "ok" and result["foreign_keys"] == 1 else 1


def command_smoke() -> int:
    with TestClient(app) as client:
        root = client.get("/")
        health = client.get("/api/system/health")
    result = {"root": root.json(), "health": health.json(), "status_codes": [root.status_code, health.status_code]}
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result["status_codes"] == [200, 200] else 1


def command_compute_demo() -> int:
    template = {
        "code": "monte-carlo-demo",
        "name": "蒙特卡洛演示",
        "algorithm": "monte-carlo",
        "parameter_schema": {
            "samples": {"type": "integer", "required": True, "minimum": 10, "maximum": 1000000},
            "seed": {"type": "integer", "required": True},
        },
        "default_parameters": {},
        "max_runtime_seconds": 60,
        "max_attempts": 3,
    }
    with TestClient(app) as client:
        created = client.post("/api/compute/templates?actor=cli-demo", json=template)
        if created.status_code not in {201, 409}:
            print(created.text)
            return 1
        task = client.post(
            "/api/compute/tasks",
            json={
                "template_code": "monte-carlo-demo",
                "project_code": "demo",
                "requested_by": "cli-user",
                "parameters": {"samples": 1000, "seed": 42},
                "priority": 80,
                "idempotency_key": "compute-demo-000001",
            },
        )
        claimed = client.post(
            "/api/compute/tasks/claim",
            json={"worker_id": "cli-worker", "capabilities": ["monte-carlo"], "lease_seconds": 60},
        )
    result = {"task": task.status_code, "claimed": claimed.status_code, "task_id": task.json().get("id")}
    print(json.dumps(result, ensure_ascii=False))
    return 0 if task.status_code == 202 and claimed.status_code == 200 and claimed.json().get("task") else 1


def command_events_demo() -> int:
    credentials = {"username": "events-demo-admin", "password": "Events!23456", "client_label": "cli-events-demo"}
    with TestClient(app) as client:
        bootstrap = client.post("/api/auth/bootstrap", json=credentials)
        if bootstrap.status_code not in {201, 409}:
            print(bootstrap.text)
            return 1
        login = client.post("/api/auth/login", json=credentials)
        if login.status_code != 200:
            print(login.text)
            return 1
        headers = {"Authorization": f"Bearer {login.json()['token']}"}
        for body in (
            {"object_type": "satellite", "object_key": "SAT-DEMO-1", "display_name": "演示卫星一号"},
            {"object_type": "mission", "object_key": "MIS-DEMO", "display_name": "演示任务"},
        ):
            client.put("/api/events/objects/catalog", json=body, headers=headers)
        field = client.post(
            "/api/events/fields/registry",
            json={"field_key": "thermal.limit_celsius", "value_type": "number", "applies_to": ["thermal_derating"]},
            headers=headers,
        )
        if field.status_code not in {201, 409}:
            print(field.text)
            return 1
        ingested = client.post(
            "/api/events",
            json={
                "source": "ttc-gateway",
                "external_id": "events-demo-000001",
                "event_type": "thermal_derating",
                "severity": "warning",
                "occurred_at": "2026-09-27T08:30:00Z",
                "summary": "载荷温度接近阈值，触发热降额",
                "satellite": "SAT-DEMO-1",
                "mission": "MIS-DEMO",
                "correlation_key": "demo-anomaly-001",
                "attributes": {"thermal.limit_celsius": 85.5},
            },
            headers=headers,
        )
        if ingested.status_code not in {200, 201}:
            print(ingested.text)
            return 1
        event_id = ingested.json()["event"]["id"]
        ack = client.post(f"/api/events/{event_id}/acknowledge", json={"reason": "值班确认"}, headers=headers)
        summary = client.get("/api/events/summary/satellite", headers=headers)
        replay = client.get("/api/events/replay/from/0?limit=10", headers=headers)
    result = {
        "event_id": event_id,
        "acknowledged": ack.status_code in {200, 409},
        "summary_groups": len(summary.json()["groups"]),
        "replayed": len(replay.json()["items"]),
    }
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result["acknowledged"] and result["summary_groups"] >= 1 and result["replayed"] >= 1 else 1


def main() -> int:
    parser = argparse.ArgumentParser(prog="compute-operations", description="科学计算任务运营服务维护入口")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("init-db", help="初始化 SQLite 数据库")
    subparsers.add_parser("check-db", help="检查数据库完整性")
    subparsers.add_parser("smoke", help="执行本地 API 冒烟检查")
    subparsers.add_parser("compute-demo", help="执行计算任务提交与领取演示")
    subparsers.add_parser("events-demo", help="执行地面运营事件中心演示")
    args = parser.parse_args()
    return {
        "init-db": command_init,
        "check-db": command_check,
        "smoke": command_smoke,
        "compute-demo": command_compute_demo,
        "events-demo": command_events_demo,
    }[args.command]()


if __name__ == "__main__":
    raise SystemExit(main())
