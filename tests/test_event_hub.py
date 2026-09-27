from __future__ import annotations

import itertools

from fastapi.testclient import TestClient

COUNTER = itertools.count(1)


def event_payload(**overrides) -> dict:
    number = next(COUNTER)
    payload = {
        "source": "ttc-gateway",
        "external_id": f"ext-{number:05d}",
        "event_type": "radiation_alert",
        "severity": "warning",
        "occurred_at": "2026-09-27T08:00:00Z",
        "summary": f"测试事件 {number}",
    }
    payload.update(overrides)
    return payload


def register_objects(client, headers) -> None:
    for object_type, key in (("satellite", "SAT-01"), ("satellite", "SAT-02"), ("mission", "MIS-A")):
        response = client.put(
            "/api/events/objects/catalog",
            json={"object_type": object_type, "object_key": key, "display_name": key},
            headers=headers,
        )
        assert response.status_code == 200, response.text


def ingest(client, headers, **overrides):
    return client.post("/api/events", json=event_payload(**overrides), headers=headers)


def make_user(client, admin, role_code: str, permissions: list[str], username: str) -> dict:
    role = client.post(
        "/api/roles",
        json={"code": role_code, "name": role_code, "description": "", "permission_codes": permissions},
        headers=admin["headers"],
    )
    assert role.status_code == 201, role.text
    user = client.post(
        "/api/users",
        json={"username": username, "password": "Viewer!23456", "display_name": username, "role_codes": [role_code]},
        headers=admin["headers"],
    )
    assert user.status_code == 201, user.text
    login = client.post("/api/auth/login", json={"username": username, "password": "Viewer!23456", "client_label": "tests"})
    assert login.status_code == 200, login.text
    return {"Authorization": f"Bearer {login.json()['token']}"}


def test_ingest_dedup_and_natural_key(client, admin):
    headers = admin["headers"]
    register_objects(client, headers)
    payload = event_payload(satellite="SAT-01", mission="MIS-A")
    first = client.post("/api/events", json=payload, headers=headers)
    assert first.status_code == 201, first.text
    assert first.json()["deduplicated"] is False
    assert first.json()["event"]["satellite"]["object_key"] == "SAT-01"
    assert first.json()["event"]["status"] == "open"

    second = client.post("/api/events", json=payload, headers=headers)
    assert second.status_code == 200
    assert second.json()["deduplicated"] is True
    assert second.json()["event"]["id"] == first.json()["event"]["id"]

    unregistered = client.post("/api/events", json=event_payload(satellite="SAT-99"), headers=headers)
    assert unregistered.status_code == 404


def test_idempotency_key_replays_response(client, admin):
    headers = admin["headers"]
    payload = event_payload()
    keyed = {**headers, "Idempotency-Key": "ingest-key-1"}
    first = client.post("/api/events", json=payload, headers=keyed)
    assert first.status_code == 201
    assert first.json()["idempotent_replay"] is False

    replay = client.post("/api/events", json=payload, headers=keyed)
    assert replay.status_code == 201
    assert replay.json()["idempotent_replay"] is True
    assert replay.json()["event"]["id"] == first.json()["event"]["id"]

    conflict = client.post("/api/events", json=event_payload(), headers=keyed)
    assert conflict.status_code == 409


def test_controlled_field_extension(client, admin):
    headers = admin["headers"]
    unknown = ingest(client, headers, attributes={"thermal.limit_celsius": 85.5})
    assert unknown.status_code == 422

    created = client.post(
        "/api/events/fields/registry",
        json={"field_key": "thermal.limit_celsius", "value_type": "number", "applies_to": ["thermal_derating"], "description": "降额温度阈值"},
        headers=headers,
    )
    assert created.status_code == 201, created.text

    wrong_type = ingest(client, headers, event_type="thermal_derating", attributes={"thermal.limit_celsius": "过热"})
    assert wrong_type.status_code == 422
    not_applicable = ingest(client, headers, event_type="radiation_alert", attributes={"thermal.limit_celsius": 85.5})
    assert not_applicable.status_code == 422
    valid = ingest(client, headers, event_type="thermal_derating", attributes={"thermal.limit_celsius": 85.5})
    assert valid.status_code == 201, valid.text
    assert valid.json()["event"]["attributes"] == {"thermal.limit_celsius": 85.5}

    enum_field = client.post(
        "/api/events/fields/registry",
        json={"field_key": "radiation.band", "value_type": "enum", "allowed_values": ["LEO", "MEO", "GEO"], "required": True, "applies_to": ["radiation_alert"]},
        headers=headers,
    )
    assert enum_field.status_code == 201
    missing_required = ingest(client, headers, event_type="radiation_alert")
    assert missing_required.status_code == 422
    invalid_value = ingest(client, headers, event_type="radiation_alert", attributes={"radiation.band": "HEO"})
    assert invalid_value.status_code == 422
    valid_enum = ingest(client, headers, event_type="radiation_alert", attributes={"radiation.band": "LEO"})
    assert valid_enum.status_code == 201

    duplicate = client.post(
        "/api/events/fields/registry",
        json={"field_key": "thermal.limit_celsius", "value_type": "number"},
        headers=headers,
    )
    assert duplicate.status_code == 409

    registry = client.get("/api/events/fields/registry", headers=headers).json()["items"]
    assert {item["field_key"] for item in registry} == {"thermal.limit_celsius", "radiation.band"}


def test_lifecycle_transitions_and_journal(client, admin):
    headers = admin["headers"]
    event = ingest(client, headers).json()["event"]

    ack = client.post(f"/api/events/{event['id']}/acknowledge", json={"reason": "值班确认"}, headers=headers)
    assert ack.status_code == 200 and ack.json()["status"] == "acknowledged"
    esc = client.post(f"/api/events/{event['id']}/escalate", json={"reason": "影响扩大"}, headers=headers)
    assert esc.status_code == 200 and esc.json()["status"] == "escalated"
    re_ack = client.post(f"/api/events/{event['id']}/acknowledge", json={"reason": "新班组确认"}, headers=headers)
    assert re_ack.status_code == 200
    closed = client.post(f"/api/events/{event['id']}/close", json={"resolution": "采取屏蔽措施后恢复"}, headers=headers)
    assert closed.status_code == 200 and closed.json()["status"] == "closed"

    duplicate_close = client.post(f"/api/events/{event['id']}/close", json={"resolution": "重复关闭"}, headers=headers)
    assert duplicate_close.status_code == 409
    archived = client.post(f"/api/events/{event['id']}/archive", json={"reason": "周报归档"}, headers=headers)
    assert archived.status_code == 200 and archived.json()["status"] == "archived"
    locked = client.post(f"/api/events/{event['id']}/acknowledge", json={"reason": "已归档"}, headers=headers)
    assert locked.status_code == 409

    journal = client.get(f"/api/events/{event['id']}/journal", headers=headers).json()["items"]
    assert [item["action"] for item in journal] == ["acknowledge", "escalate", "acknowledge", "close", "archive"]
    assert all(item["actor"] == "admin" for item in journal)
    assert journal[0]["before"]["status"] == "open"
    assert journal[0]["after"]["status"] == "acknowledged"


def test_batch_archive_reports_per_item(client, admin):
    headers = admin["headers"]
    closed_ids = []
    for _ in range(2):
        event = ingest(client, headers).json()["event"]
        client.post(f"/api/events/{event['id']}/close", json={"resolution": "处理完成"}, headers=headers)
        closed_ids.append(event["id"])
    open_id = ingest(client, headers).json()["event"]["id"]

    result = client.post(
        "/api/events/archive-batch",
        json={"event_ids": closed_ids + [open_id], "reason": "交接班批量归档"},
        headers=headers,
    )
    assert result.status_code == 200, result.text
    body = result.json()
    assert sorted(body["succeeded"]) == sorted(closed_ids)
    assert [item["event_id"] for item in body["failed"]] == [open_id]

    journal = client.get(f"/api/events/{closed_ids[0]}/journal", headers=headers).json()["items"]
    archive_entries = [item for item in journal if item["action"] == "archive"]
    assert archive_entries and archive_entries[0]["batch_key"] == body["batch_key"]


def test_correction_and_delete_leave_audit_trail(client, admin):
    headers = admin["headers"]
    register_objects(client, headers)
    event = ingest(client, headers, satellite="SAT-01", summary="原始摘要", severity="warning").json()["event"]

    corrected = client.post(
        f"/api/events/{event['id']}/corrections",
        json={"reason": "值班复核更正", "summary": "修正摘要", "severity": "critical", "satellite": "SAT-02"},
        headers=headers,
    )
    assert corrected.status_code == 200, corrected.text
    body = corrected.json()
    assert body["summary"] == "修正摘要" and body["severity"] == "critical"
    assert body["satellite"]["object_key"] == "SAT-02"
    assert body["version"] == event["version"] + 1

    journal = client.get(f"/api/events/{event['id']}/journal", headers=headers).json()["items"]
    entry = journal[-1]
    assert entry["action"] == "correct"
    assert entry["actor"] == "admin"
    assert entry["reason"] == "值班复核更正"
    assert entry["before"]["changed"]["summary"] == "原始摘要"
    assert entry["after"]["changed"]["summary"] == "修正摘要"
    assert entry["before"]["changed"]["severity"] == "warning"

    noop = client.post(
        f"/api/events/{event['id']}/corrections",
        json={"reason": "无实际变化", "summary": "修正摘要"},
        headers=headers,
    )
    assert noop.status_code == 409

    deleted = client.request("DELETE", f"/api/events/{event['id']}", json={"reason": "误报事件"}, headers=headers)
    assert deleted.status_code == 200
    assert client.get(f"/api/events/{event['id']}", headers=headers).status_code == 404
    visible = client.get(f"/api/events/{event['id']}?include_deleted=true", headers=headers)
    assert visible.status_code == 200 and visible.json()["deleted_at"]
    timeline = client.get("/api/events", headers=headers).json()
    assert all(item["id"] != event["id"] for item in timeline["items"])

    journal = client.get(f"/api/events/{event['id']}/journal", headers=headers).json()["items"]
    delete_entry = journal[-1]
    assert delete_entry["action"] == "delete" and delete_entry["actor"] == "admin"
    assert delete_entry["before"]["deleted_at"] is None
    assert delete_entry["after"]["deleted_at"]

    restored = client.post(f"/api/events/{event['id']}/restore", json={"reason": "复核后保留"}, headers=headers)
    assert restored.status_code == 200 and restored.json()["deleted_at"] is None
    duplicate_ingest = client.post("/api/events", json=event_payload(external_id=event["external_id"]), headers=headers)
    assert duplicate_ingest.status_code == 200


def test_permissions_are_enforced(client, admin):
    headers = admin["headers"]
    assert client.get("/api/events").status_code == 401

    viewer = make_user(client, admin, "event-viewer", ["events.read"], "viewer01")
    operator = make_user(client, admin, "event-operator", ["events.read", "events.operate"], "operator01")

    assert client.get("/api/events", headers=viewer).status_code == 200
    assert client.post("/api/events", json=event_payload(), headers=viewer).status_code == 403

    event = ingest(client, headers).json()["event"]
    assert client.post(f"/api/events/{event['id']}/acknowledge", json={"reason": "越权确认"}, headers=viewer).status_code == 403
    assert client.post("/api/events", json=event_payload(), headers=operator).status_code == 403
    allowed = client.post(f"/api/events/{event['id']}/acknowledge", json={"reason": "值班确认"}, headers=operator)
    assert allowed.status_code == 200


def test_timeline_filters_and_invalid_status(client, admin):
    headers = admin["headers"]
    register_objects(client, headers)
    ingest(client, headers, event_type="launch", severity="info", satellite="SAT-01", mission="MIS-A")
    ingest(client, headers, event_type="radiation_alert", severity="critical", satellite="SAT-02")

    by_satellite = client.get("/api/events?satellite=SAT-01", headers=headers).json()
    assert len(by_satellite["items"]) == 1 and by_satellite["items"][0]["event_type"] == "launch"
    by_severity = client.get("/api/events?status=open&severity=critical", headers=headers).json()
    assert len(by_severity["items"]) == 1
    by_type = client.get("/api/events?event_type=radiation_alert", headers=headers).json()
    assert len(by_type["items"]) == 1 and by_type["items"][0]["satellite"]["object_key"] == "SAT-02"
    assert client.get("/api/events?status=bogus", headers=headers).status_code == 422


def test_cursor_pagination_stable_under_inserts(client, admin):
    headers = admin["headers"]
    ids = []
    for index in range(5):
        response = ingest(client, headers, occurred_at=f"2026-09-27T08:0{index}:00Z")
        assert response.status_code == 201
        ids.append(response.json()["event"]["id"])

    first = client.get("/api/events?sort=ingested&limit=2", headers=headers).json()
    assert [item["id"] for item in first["items"]] == [ids[4], ids[3]]
    assert first["has_more"] is True

    # 翻页间隙写入新事件，不应导致已翻页范围重复或漏项
    ingest(client, headers, occurred_at="2026-09-27T09:00:00Z")

    second = client.get(f"/api/events?sort=ingested&limit=2&cursor={first['next_cursor']}", headers=headers).json()
    assert [item["id"] for item in second["items"]] == [ids[2], ids[1]]
    third = client.get(f"/api/events?sort=ingested&limit=2&cursor={second['next_cursor']}", headers=headers).json()
    assert [item["id"] for item in third["items"]] == [ids[0]]
    assert third["has_more"] is False

    collected = [item["id"] for item in first["items"] + second["items"] + third["items"]]
    assert sorted(collected) == sorted(ids)
    assert len(collected) == len(set(collected))

    occurred_first = client.get("/api/events?sort=occurred&limit=3", headers=headers).json()
    assert [item["id"] for item in occurred_first["items"]] == [ids[4] + 1, ids[4], ids[3]]
    occurred_rest = client.get(f"/api/events?sort=occurred&limit=10&cursor={occurred_first['next_cursor']}", headers=headers).json()
    assert [item["id"] for item in occurred_rest["items"]] == [ids[2], ids[1], ids[0]]

    mismatch = client.get(f"/api/events?sort=ingested&limit=2&source=other&cursor={first['next_cursor']}", headers=headers)
    assert mismatch.status_code == 422
    garbage = client.get("/api/events?sort=ingested&cursor=not-a-cursor", headers=headers)
    assert garbage.status_code == 422


def test_replay_from_and_named_cursor_survive_restart(client, admin):
    headers = admin["headers"]
    ids = [ingest(client, headers).json()["event"]["id"] for _ in range(3)]

    replay = client.get("/api/events/replay/from/0?limit=2", headers=headers).json()
    assert [item["id"] for item in replay["items"]] == ids[:2]
    assert replay["has_more"] is True
    rest = client.get(f"/api/events/replay/from/{replay['last_event_id']}?limit=10", headers=headers).json()
    assert [item["id"] for item in rest["items"]] == ids[2:]
    assert rest["has_more"] is False

    created = client.put("/api/events/cursors/named/shift-a", json={"from_event_id": 0}, headers=headers)
    assert created.status_code == 200, created.text
    page = client.post("/api/events/cursors/named/shift-a/next?limit=2", headers=headers).json()
    assert [item["id"] for item in page["items"]] == ids[:2]
    assert page["last_event_id"] == ids[1]

    extra = ingest(client, headers).json()["event"]["id"]

    # 模拟服务重启：关闭连接并以新的客户端重新加载应用，游标位置从数据库恢复
    from app.database import close_connection
    from app.main import app

    close_connection()
    with TestClient(app) as restarted:
        resumed = restarted.post("/api/events/cursors/named/shift-a/next?limit=10", headers=headers)
        assert resumed.status_code == 200, resumed.text
        assert [item["id"] for item in resumed.json()["items"]] == [ids[2], extra]
        drained = restarted.post("/api/events/cursors/named/shift-a/next?limit=10", headers=headers).json()
        assert drained["items"] == [] and drained["advanced"] is False
        position = restarted.get("/api/events/cursors/named/shift-a", headers=headers).json()
        assert position["last_event_id"] == extra


def test_summary_by_satellite_and_mission(client, admin):
    headers = admin["headers"]
    register_objects(client, headers)
    launch = ingest(client, headers, event_type="launch", severity="info", satellite="SAT-01", mission="MIS-A", summary="发射").json()["event"]
    ingest(client, headers, event_type="radiation_alert", severity="critical", satellite="SAT-01", mission="MIS-A")
    derating = ingest(client, headers, event_type="thermal_derating", severity="warning", satellite="SAT-02", mission="MIS-A").json()["event"]
    client.post(f"/api/events/{derating['id']}/acknowledge", json={"reason": "已知晓"}, headers=headers)
    client.post(f"/api/events/{launch['id']}/close", json={"resolution": "发射阶段完成"}, headers=headers)

    satellites = client.get("/api/events/summary/satellite", headers=headers).json()
    by_key = {group["object"]["object_key"]: group for group in satellites["groups"]}
    assert by_key["SAT-01"]["total"] == 2
    assert by_key["SAT-01"]["by_status"]["closed"] == 1
    assert by_key["SAT-01"]["critical_active_count"] == 1
    assert by_key["SAT-01"]["last_event"]["event_type"] == "radiation_alert"
    assert by_key["SAT-02"]["by_status"]["acknowledged"] == 1
    assert by_key["SAT-02"]["active_count"] == 1

    missions = client.get("/api/events/summary/mission", headers=headers).json()
    assert len(missions["groups"]) == 1
    mission_group = missions["groups"][0]
    assert mission_group["total"] == 3
    assert mission_group["by_type"]["launch"] == 1
    assert mission_group["active_count"] == 2

    overview = client.get("/api/events/overview", headers=headers).json()
    assert overview["active_count"] == 2
    assert overview["critical_active_count"] == 1
    assert overview["status_counts"]["closed"] == 1


def test_links_and_correlation(client, admin):
    headers = admin["headers"]
    alert = ingest(client, headers, correlation_key="anomaly-001").json()["event"]
    handling = ingest(
        client, headers, event_type="manual_intervention", correlation_key="anomaly-001", summary="人工处置：调整姿态"
    ).json()["event"]

    link = client.post(
        f"/api/events/{handling['id']}/links",
        json={"related_event_id": alert["id"], "link_type": "resolution"},
        headers=headers,
    )
    assert link.status_code == 201, link.text
    duplicate = client.post(
        f"/api/events/{handling['id']}/links",
        json={"related_event_id": alert["id"], "link_type": "resolution"},
        headers=headers,
    )
    assert duplicate.status_code == 409
    self_link = client.post(f"/api/events/{alert['id']}/links", json={"related_event_id": alert["id"]}, headers=headers)
    assert self_link.status_code == 422

    detail = client.get(f"/api/events/{alert['id']}", headers=headers).json()
    counterparts = {
        item["event_id"] if item["related_event_id"] == alert["id"] else item["related_event_id"]
        for item in detail["links"]
    }
    assert handling["id"] in counterparts
    assert [item["id"] for item in detail["correlated"]] == [handling["id"]]

    filtered = client.get("/api/events?correlation_key=anomaly-001", headers=headers).json()
    assert {item["id"] for item in filtered["items"]} == {alert["id"], handling["id"]}


def test_batch_ingest_mixed_results(client, admin):
    headers = admin["headers"]
    existing = ingest(client, headers).json()["event"]
    items = [
        event_payload(),
        event_payload(external_id=existing["external_id"]),
        event_payload(attributes={"unknown.field": 1}),
    ]
    response = client.post("/api/events/batch", json={"items": items}, headers=headers)
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["created_count"] == 1
    assert body["duplicate_count"] == 1
    assert body["rejected_count"] == 1
    assert body["rejected"][0]["code"] == "validation_error"
