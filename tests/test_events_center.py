from __future__ import annotations

from fastapi.testclient import TestClient

from app.database import close_connection
from app.main import app


def ingest(client, headers, **overrides):
    payload = {
        "event_type": "launch",
        "satellite_code": "SAT-1",
        "mission_code": "M-1",
        "source": "ground-ccs",
        "severity": "info",
        "title": "火箭发射",
        "idempotency_key": "",
        "extra": {},
    }
    payload.update(overrides)
    return client.post("/api/events", json=payload, headers=headers)


def restricted_user(client, admin_headers):
    """创建一个没有任何 events 权限的普通账号。"""
    role = client.post(
        "/api/roles",
        json={"code": "spectator", "name": "旁观角色", "permission_codes": []},
        headers=admin_headers,
    )
    assert role.status_code == 201, role.text
    user = client.post(
        "/api/users",
        json={"username": "spectator01", "password": "Viewer!234567", "display_name": "值班旁观", "role_codes": ["spectator"]},
        headers=admin_headers,
    )
    assert user.status_code == 201, user.text
    login = client.post("/api/auth/login", json={"username": "spectator01", "password": "Viewer!234567", "client_label": "t"})
    assert login.status_code == 200
    return {"Authorization": f"Bearer {login.json()['token']}"}


def test_requires_authentication_and_permission(client, admin):
    # 未携带令牌
    assert client.get("/api/events").status_code == 401
    denied = restricted_user(client, admin["headers"])
    assert client.get("/api/events", headers=denied).status_code == 403
    assert client.post(
        "/api/events",
        json={"event_type": "launch", "satellite_code": "S", "source": "src", "title": "t"},
        headers=denied,
    ).status_code == 403
    # 管理员（拥有 *）可访问并看到内置事件类型
    types = client.get("/api/events/types", headers=admin["headers"])
    assert types.status_code == 200
    codes = {item["code"] for item in types.json()}
    assert {"launch", "orbit_insertion", "thermal_derating", "radiation_alert", "mission_failure", "manual_action"} <= codes


def test_ingest_timeline_ordering_and_history_replay(client, admin):
    headers = admin["headers"]
    # 历史回放：先写入较早发生的事件
    older = ingest(client, headers, title="入轨", event_type="orbit_insertion",
                   occurred_at="2026-09-20T02:00:00+00:00", idempotency_key="hist-0001")
    newer = ingest(client, headers, title="发射后遥测", occurred_at="2026-09-26T01:00:00+00:00",
                   idempotency_key="hist-0002")
    assert older.status_code == 201 and newer.status_code == 201
    # 时间线按接入序号（seq）升序，历史事件先入库序号更小
    page = client.get("/api/events?limit=50", headers=headers).json()
    titles = [item["title"] for item in page["items"]]
    assert titles == ["入轨", "发射后遥测"]
    # occurred_at 过滤
    filtered = client.get("/api/events?occurred_from=2026-09-25T00:00:00%2B00:00", headers=headers).json()
    assert [item["title"] for item in filtered["items"]] == ["发射后遥测"]


def test_idempotent_write_returns_same_event_and_rejects_different_payload(client, admin):
    headers = admin["headers"]
    first = ingest(client, headers, idempotency_key="idem-0001", title="辐射峰值 32krad")
    assert first.status_code == 201
    second = ingest(client, headers, idempotency_key="idem-0001", title="辐射峰值 32krad")
    assert second.status_code == 200
    assert second.json()["replayed"] is True
    assert second.json()["event"]["seq"] == first.json()["event"]["seq"]
    assert client.get("/api/events/summary", headers=headers).json()["total_events"] == 1
    # 同一幂等键不同内容 -> 冲突
    clash = ingest(client, headers, idempotency_key="idem-0001", title="完全不同的内容")
    assert clash.status_code == 409


def test_dedup_window_collapses_same_payload(client, admin):
    headers = admin["headers"]
    common = {"idempotency_key": "", "dedup_window_seconds": 300, "title": "热降额", "event_type": "thermal_derating", "severity": "warning"}
    first = ingest(client, headers, occurred_at="2026-09-27T00:00:00+00:00", **common)
    # 5 分钟窗口内、同载荷的重复上报被折叠
    dup = ingest(client, headers, occurred_at="2026-09-27T00:03:00+00:00", **common)
    assert first.status_code == 201
    assert dup.status_code == 200
    assert dup.json()["deduplicated"] is True
    assert dup.json()["event"]["seq"] == first.json()["event"]["seq"]
    # 窗口外相同内容保留为新事件
    later = ingest(client, headers, occurred_at="2026-09-27T02:00:00+00:00", **common)
    assert later.status_code == 201


def test_controlled_extra_fields_validated(client, admin):
    headers = admin["headers"]
    created = client.post(
        "/api/events/types",
        json={
            "code": "battery_anomaly",
            "name": "电池异常",
            "lifecycle": "alert",
            "extra_schema": {
                "cell_temp_c": {"type": "number", "required": True, "minimum": -40, "maximum": 120},
                "mode": {"type": "string", "choices": ["charge", "discharge"]},
            },
        },
        headers=headers,
    )
    assert created.status_code == 201, created.text
    ok = ingest(client, headers, event_type="battery_anomaly", title="电池温度异常",
                extra={"cell_temp_c": 88.2, "mode": "charge"}, idempotency_key="ext-0001")
    assert ok.status_code == 201, ok.text
    assert ok.json()["event"]["extra"] == {"cell_temp_c": 88.2, "mode": "charge"}
    # 未声明字段
    rejected = ingest(client, headers, event_type="battery_anomaly", title="x",
                      extra={"cell_temp_c": 50, "unknown": 1}, idempotency_key="ext-0002")
    assert rejected.status_code == 422
    # 缺少必填
    missing = ingest(client, headers, event_type="battery_anomaly", title="x", extra={}, idempotency_key="ext-0003")
    assert missing.status_code == 422
    # 超出范围
    out_of_range = ingest(client, headers, event_type="battery_anomaly", title="x",
                          extra={"cell_temp_c": 200}, idempotency_key="ext-0004")
    assert out_of_range.status_code == 422
    # 未注册类型
    unknown_type = ingest(client, headers, event_type="not_registered", title="x", idempotency_key="ext-0005")
    assert unknown_type.status_code == 404


def test_cursor_pagination_stable_under_concurrent_writes(client, admin):
    headers = admin["headers"]
    for index in range(5):
        response = ingest(client, headers, title=f"事件{index}", idempotency_key=f"page-{index:04d}")
        assert response.status_code == 201
    first = client.get("/api/events?limit=2", headers=headers).json()
    assert [item["title"] for item in first["items"]] == ["事件0", "事件1"]
    assert first["page"]["has_more"] is True
    cursor = first["page"]["next_cursor"]
    # 翻页期间又写入新事件
    for index in range(5, 8):
        ingest(client, headers, title=f"事件{index}", idempotency_key=f"page-{index:04d}")
    second = client.get(f"/api/events?limit=2&cursor={cursor}", headers=headers).json()
    # 不重复、不漏项：仍从 seq=2 之后继续
    assert [item["title"] for item in second["items"]] == ["事件2", "事件3"]
    third = client.get(f"/api/events?limit=2&cursor={second['page']['next_cursor']}", headers=headers).json()
    assert [item["title"] for item in third["items"]] == ["事件4", "事件5"]
    # 查询条件变化时旧游标失效
    tampered = client.get(f"/api/events?limit=2&cursor={cursor}&satellite_code=OTHER", headers=headers)
    assert tampered.status_code == 422


def test_cursor_survives_service_restart(tmp_path, client, admin):
    headers = admin["headers"]
    for index in range(3):
        ingest(client, headers, title=f"重启前{index}", idempotency_key=f"restart-{index:04d}")
    page = client.get("/api/events?limit=1", headers=headers).json()
    cursor = page["page"]["next_cursor"]
    # 模拟服务重启：关闭线程连接，重新通过 lifespan 初始化同一数据库文件
    close_connection()
    with TestClient(app) as restarted:
        response = restarted.get(f"/api/events?limit=2&cursor={cursor}", headers=headers)
        assert response.status_code == 200, response.text
        assert [item["title"] for item in response.json()["items"]] == ["重启前1", "重启前2"]


def test_lifecycle_ack_escalate_resolve_close_and_illegal_transitions(client, admin):
    headers = admin["headers"]
    alert = ingest(client, headers, event_type="radiation_alert", severity="critical",
                   title="太阳质子事件", idempotency_key="life-0001").json()["event"]
    seq = alert["seq"]
    # 未解决不能关闭
    assert client.post(f"/api/events/{seq}/close", json={"reason": "直接关闭"}, headers=headers).status_code == 409
    acked = client.post(f"/api/events/{seq}/acknowledge", json={"note": "值班员已看到"}, headers=headers)
    assert acked.status_code == 200 and acked.json()["status"] == "acknowledged"
    # 重复确认被拒
    assert client.post(f"/api/events/{seq}/acknowledge", json={}, headers=headers).status_code == 409
    escalated = client.post(f"/api/events/{seq}/escalate", json={"reason": "剂量率超阈值", "level": 3}, headers=headers)
    assert escalated.status_code == 200
    assert escalated.json()["status"] == "escalated" and escalated.json()["escalation_level"] == 3
    resolved = client.post(f"/api/events/{seq}/resolve", json={"reason": "辐射环境恢复"}, headers=headers)
    assert resolved.json()["status"] == "resolved"
    closed = client.post(f"/api/events/{seq}/close", json={"reason": "事件闭环"}, headers=headers)
    assert closed.json()["status"] == "closed"
    assert closed.json()["closed_by"] == "系统管理员"
    # 通告类事件允许不经解决直接关闭
    advisory = ingest(client, headers, event_type="orbit_insertion", title="入轨点确认", idempotency_key="life-0002")
    seq2 = advisory.json()["event"]["seq"]
    assert client.post(f"/api/events/{seq2}/close", json={"reason": "通告留档"}, headers=headers).status_code == 200


def test_correction_and_delete_leave_before_after_with_actor(client, admin):
    headers = admin["headers"]
    event = ingest(client, headers, title="任务失败-初报", severity="warning",
                   idempotency_key="audit-0001").json()["event"]
    seq = event["seq"]
    corrected = client.patch(
        f"/api/events/{seq}",
        json={"reason": "地面站复核修正级别", "severity": "critical", "title": "任务失败-确认"},
        headers=headers,
    )
    assert corrected.status_code == 200
    assert corrected.json()["severity"] == "critical"
    changes = client.get(f"/api/events/{seq}/changes", headers=headers).json()["changes"]
    correction = [item for item in changes if item["action"] == "correct"][0]
    assert correction["actor"] == "系统管理员"
    assert correction["before_json"] is not None and correction["after_json"] is not None
    # 删除保留墓碑：事件已不可查，但按 seq 仍能查到删除前后值和操作者
    deleted = client.request("DELETE", f"/api/events/{seq}", json={"reason": "误录事件，按规删除"}, headers=headers)
    assert deleted.status_code == 200 and deleted.json()["deleted"] is True
    assert client.get(f"/api/events/{seq}", headers=headers).status_code == 404
    tombstone = client.get(f"/api/events/{seq}/changes", headers=headers).json()["changes"]
    delete_log = [item for item in tombstone if item["action"] == "delete"][0]
    assert delete_log["actor"] == "系统管理员"
    assert delete_log["reason"] == "误录事件，按规删除"
    assert delete_log["before_json"] is not None and delete_log["after_json"] is None


def test_event_links_are_directional_and_visible_from_both_sides(client, admin):
    headers = admin["headers"]
    cause = ingest(client, headers, event_type="radiation_alert", title="辐射告警",
                   idempotency_key="link-0001").json()["event"]
    effect = ingest(client, headers, event_type="thermal_derating", title="热降额处置",
                    idempotency_key="link-0002").json()["event"]
    response = client.post(
        f"/api/events/{effect['seq']}/links",
        json={"linked_event_seq": cause["seq"], "relation": "caused_by", "note": "辐射导致降额"},
        headers=headers,
    )
    assert response.status_code == 201, response.text
    assert response.json()["links"][0]["direction"] == "outgoing"
    # 从原因端可见入向关联
    back = client.get(f"/api/events/{cause['seq']}", headers=headers).json()
    incoming = [link for link in back["links"] if link["direction"] == "incoming"]
    assert len(incoming) == 1 and incoming[0]["relation"] == "caused_by"
    # 重复关联被拒绝
    duplicate = client.post(
        f"/api/events/{effect['seq']}/links",
        json={"linked_event_seq": cause["seq"], "relation": "caused_by"},
        headers=headers,
    )
    assert duplicate.status_code == 409


def test_subjects_and_object_timeline(client, admin):
    headers = admin["headers"]
    ingest(client, headers, title="发射", idempotency_key="sub-0001",
           subjects=[{"object_type": "vehicle", "object_key": "CZ-5A-Y9", "role": "launch_vehicle", "label": "长征五号甲"}])
    ingest(client, headers, title="入轨", idempotency_key="sub-0002",
           subjects=[{"object_type": "vehicle", "object_key": "CZ-5A-Y9"}])
    objects = client.get("/api/events/objects/vehicle/CZ-5A-Y9", headers=headers).json()
    assert len(objects["items"]) == 2
    detail = client.get("/api/events/1", headers=headers).json()
    assert detail["subjects"][0]["object_key"] == "CZ-5A-Y9"


def test_batch_archive_only_terminal_and_idempotent(client, admin):
    headers = admin["headers"]
    open_event = ingest(client, headers, title="未关闭告警", event_type="radiation_alert",
                        idempotency_key="arch-0001").json()["event"]
    done_event = ingest(client, headers, title="已解决事件", idempotency_key="arch-0002").json()["event"]
    client.post(f"/api/events/{done_event['seq']}/resolve", json={"reason": "处置完成"}, headers=headers)
    batch = client.post(
        "/api/events/archive",
        json={"statuses": ["resolved", "closed"], "batch_key": "archive-nightly-001"},
        headers=headers,
    )
    assert batch.status_code == 200
    assert batch.json()["archived"] == 1
    assert done_event["seq"] in batch.json()["event_seqs"]
    assert open_event["seq"] not in batch.json()["event_seqs"]
    # 同批次键重放不重复归档
    replay = client.post(
        "/api/events/archive",
        json={"statuses": ["resolved", "closed"], "batch_key": "archive-nightly-001"},
        headers=headers,
    )
    assert replay.json()["replayed"] is True and replay.json()["archived"] == 0
    # 进行中状态不允许作为归档条件
    forbidden = client.post("/api/events/archive", json={"statuses": ["open"]}, headers=headers)
    assert forbidden.status_code == 422


def test_satellite_and_mission_summaries(client, admin):
    headers = admin["headers"]
    ingest(client, headers, satellite_code="SAT-A", mission_code="M-X", event_type="launch",
           title="A 星发射", idempotency_key="sum-0001")
    alert = ingest(client, headers, satellite_code="SAT-A", mission_code="M-X",
                   event_type="radiation_alert", severity="critical", title="A 星告警",
                   idempotency_key="sum-0002").json()["event"]
    ingest(client, headers, satellite_code="SAT-B", mission_code="M-Y", title="B 星发射",
           idempotency_key="sum-0003")
    client.post(f"/api/events/{alert['seq']}/acknowledge", json={"note": "已确认"}, headers=headers)

    satellite = client.get("/api/events/summary/satellites/SAT-A", headers=headers).json()
    assert satellite["total_events"] == 2
    assert satellite["by_type"]["launch"] == 1
    assert len(satellite["open_alerts"]) == 1
    mission = client.get("/api/events/summary/missions/M-X", headers=headers).json()
    assert mission["satellite_count"] == 1
    assert [item["title"] for item in mission["timeline"]] == ["A 星发射", "A 星告警"]
    assert client.get("/api/events/summary/satellites/NOPE", headers=headers).status_code == 404
