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
    events = [
        {"event_type": "launch", "satellite_code": "SAT-DEMO", "mission_code": "M-DEMO", "source": "ground-ccs",
         "severity": "info", "title": "运载火箭发射", "idempotency_key": "events-demo-000001"},
        {"event_type": "orbit_insertion", "satellite_code": "SAT-DEMO", "mission_code": "M-DEMO", "source": "orbit-control",
         "severity": "info", "title": "太阳同步轨道入轨确认", "idempotency_key": "events-demo-000002"},
        {"event_type": "radiation_alert", "satellite_code": "SAT-DEMO", "mission_code": "M-DEMO", "source": "space-weather",
         "severity": "critical", "title": "太阳质子事件辐射告警", "idempotency_key": "events-demo-000003"},
    ]
    with TestClient(app) as client:
        bootstrapped = client.post("/api/auth/bootstrap", json={"username": "admin", "password": "Admin!23456", "client_label": "events-demo"})
        if bootstrapped.status_code not in {201, 409}:
            print(bootstrapped.text)
            return 1
        login = client.post("/api/auth/login", json={"username": "admin", "password": "Admin!23456", "client_label": "events-demo"})
        if login.status_code != 200:
            print(login.text)
            return 1
        headers = {"Authorization": f"Bearer {login.json()['token']}"}
        statuses = []
        seqs = []
        for event in events:
            response = client.post("/api/events", json=event, headers=headers)
            if response.status_code not in {200, 201}:
                print(response.text)
                return 1
            statuses.append(response.status_code)
            seqs.append(response.json()["event"]["seq"])
        timeline = client.get("/api/events?satellite_code=SAT-DEMO&limit=10", headers=headers)
        summary = client.get("/api/events/summary/satellites/SAT-DEMO", headers=headers)
    result = {
        "ingest_statuses": statuses,
        "timeline_events": len(timeline.json()["items"]),
        "satellite_summary": summary.json().get("by_status"),
        "next_cursor_present": bool(timeline.json()["page"]["next_cursor"]),
    }
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result["timeline_events"] == 3 else 1


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
