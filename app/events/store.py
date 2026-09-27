from __future__ import annotations

import json
import sqlite3
from typing import Any, Iterable

from app.database import get_connection

# 内置事件类型：地面运营时间线的受控词表，可通过 event_type_defs 受控扩展。
BUILTIN_EVENT_TYPES: tuple[str, ...] = (
    "launch",          # 发射
    "orbit_insertion", # 入轨
    "thermal_derating",# 热降额
    "radiation_alert", # 辐射告警
    "mission_failure", # 任务失败
    "manual_action",   # 人工处置
)

# 事件生命周期。lifecycle 决定允许的状态迁移，禁止随意关闭未确认的告警。
BUILTIN_LIFECYCLES: tuple[str, ...] = ("advisory", "alert", "actionable")

SCHEMA = r"""
CREATE TABLE IF NOT EXISTS event_type_defs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    category TEXT NOT NULL DEFAULT 'operational',
    lifecycle TEXT NOT NULL DEFAULT 'advisory' CHECK(lifecycle IN ('advisory','alert','actionable')),
    description TEXT NOT NULL DEFAULT '',
    -- 受控扩展字段：名称 -> JSON 规则（类型、是否必填、取值范围/枚举）
    extra_schema_json TEXT NOT NULL DEFAULT '{}',
    is_builtin INTEGER NOT NULL DEFAULT 0 CHECK(is_builtin IN (0,1)),
    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)),
    created_by TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS operational_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    -- 稳定全局序号：单调递增，游标分页的唯一排序依据，永不复用
    seq INTEGER NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    satellite_code TEXT NOT NULL,
    mission_code TEXT NOT NULL DEFAULT '',
    source TEXT NOT NULL DEFAULT '',
    severity TEXT NOT NULL DEFAULT 'info' CHECK(severity IN ('info','warning','critical')),
    status TEXT NOT NULL DEFAULT 'open' CHECK(status IN ('open','acknowledged','escalated','resolved','closed','archived')),
    title TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    occurred_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    -- 受控扩展字段的实际取值
    extra_json TEXT NOT NULL DEFAULT '{}',
    -- 幂等写入：来源系统 + 幂等键唯一；指纹不同则冲突
    idempotency_key TEXT NOT NULL DEFAULT '',
    payload_digest TEXT NOT NULL DEFAULT '',
    dedup_window_seconds INTEGER NOT NULL DEFAULT 0,
    -- 关联主线（根事件）；关联事件通过 event_links 多对多挂载
    root_event_id INTEGER REFERENCES operational_events(id) ON DELETE SET NULL,
    acknowledged_by TEXT NOT NULL DEFAULT '',
    acknowledged_at TEXT,
    escalated_by TEXT NOT NULL DEFAULT '',
    escalated_at TEXT,
    resolved_by TEXT NOT NULL DEFAULT '',
    resolved_at TEXT,
    closed_by TEXT NOT NULL DEFAULT '',
    closed_at TEXT,
    archived_by TEXT NOT NULL DEFAULT '',
    archived_at TEXT,
    escalation_level INTEGER NOT NULL DEFAULT 0,
    archive_batch TEXT NOT NULL DEFAULT '',
    version INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_op_events_seq ON operational_events(seq);
CREATE INDEX IF NOT EXISTS idx_op_events_timeline ON operational_events(occurred_at,id);
CREATE INDEX IF NOT EXISTS idx_op_events_satellite ON operational_events(satellite_code,occurred_at);
CREATE INDEX IF NOT EXISTS idx_op_events_mission ON operational_events(mission_code,occurred_at);
CREATE INDEX IF NOT EXISTS idx_op_events_status ON operational_events(status,occurred_at);
CREATE INDEX IF NOT EXISTS idx_op_events_type ON operational_events(event_type,occurred_at);
CREATE UNIQUE INDEX IF NOT EXISTS idx_op_events_idempotency
    ON operational_events(source,idempotency_key) WHERE idempotency_key <> '';

CREATE TABLE IF NOT EXISTS event_links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id INTEGER NOT NULL REFERENCES operational_events(id) ON DELETE CASCADE,
    linked_event_id INTEGER NOT NULL REFERENCES operational_events(id) ON DELETE CASCADE,
    relation TEXT NOT NULL CHECK(relation IN ('related','caused_by','duplicates','succeeds','blocks')),
    note TEXT NOT NULL DEFAULT '',
    created_by TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    UNIQUE(event_id,linked_event_id,relation),
    CHECK(event_id <> linked_event_id)
);
CREATE INDEX IF NOT EXISTS idx_event_links_event ON event_links(event_id);
CREATE INDEX IF NOT EXISTS idx_event_links_linked ON event_links(linked_event_id);

CREATE TABLE IF NOT EXISTS event_subjects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id INTEGER NOT NULL REFERENCES operational_events(id) ON DELETE CASCADE,
    object_type TEXT NOT NULL,
    object_key TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'related',
    label TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    UNIQUE(event_id,object_type,object_key)
);
CREATE INDEX IF NOT EXISTS idx_event_subjects_object ON event_subjects(object_type,object_key);

-- 更正/删除/状态流转审计：删除或更正必须留下前后值和操作者
CREATE TABLE IF NOT EXISTS event_change_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id INTEGER REFERENCES operational_events(id) ON DELETE SET NULL,
    seq INTEGER,
    action TEXT NOT NULL CHECK(action IN ('create','correct','delete','acknowledge','escalate','resolve','close','archive','relate','unrelate')),
    actor TEXT NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    before_json TEXT,
    after_json TEXT,
    batch_key TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_event_changes_event ON event_change_logs(event_id,id);
CREATE INDEX IF NOT EXISTS idx_event_changes_seq ON event_change_logs(seq);

CREATE TABLE IF NOT EXISTS event_archives (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_key TEXT NOT NULL UNIQUE,
    criteria_json TEXT NOT NULL,
    amount INTEGER NOT NULL,
    archived_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"""


def ensure_schema(connection: sqlite3.Connection | None = None) -> None:
    (connection or get_connection()).executescript(SCHEMA)


class EventStore:
    """事件中心的 SQLite 读写封装。"""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection

    # ---- 类型定义（受控扩展） ----

    def seed_builtin_types(self, now: str) -> None:
        builtins = {
            "launch": ("发射", "advisory"),
            "orbit_insertion": ("入轨", "advisory"),
            "thermal_derating": ("热降额", "alert"),
            "radiation_alert": ("辐射告警", "alert"),
            "mission_failure": ("任务失败", "actionable"),
            "manual_action": ("人工处置", "actionable"),
        }
        for code, (name, lifecycle) in builtins.items():
            self.connection.execute(
                "INSERT OR IGNORE INTO event_type_defs(code,name,category,lifecycle,is_builtin,created_at,updated_at) VALUES(?,?, 'operational',?,1,?,?)",
                (code, name, lifecycle, now, now),
            )

    def type_def(self, code: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM event_type_defs WHERE code=?", (code,)).fetchone()

    def list_type_defs(self, *, include_inactive: bool = False) -> list[dict[str, Any]]:
        sql = "SELECT * FROM event_type_defs"
        if not include_inactive:
            sql += " WHERE active=1"
        sql += " ORDER BY is_builtin DESC,code"
        return [dict(row) for row in self.connection.execute(sql).fetchall()]

    def create_type_def(self, *, code: str, name: str, category: str, lifecycle: str, description: str, extra_schema: dict[str, Any], created_by: str, now: str) -> dict[str, Any]:
        cursor = self.connection.execute(
            "INSERT INTO event_type_defs(code,name,category,lifecycle,description,extra_schema_json,is_builtin,active,created_by,created_at,updated_at) VALUES(?,?,?,?,?,?,0,1,?,?,?)",
            (code, name, category, lifecycle, description, json.dumps(extra_schema, ensure_ascii=False, sort_keys=True), created_by, now, now),
        )
        return dict(self.type_def_by_id(cursor.lastrowid))

    def update_type_def(self, code: str, *, name: str | None, description: str | None, extra_schema: dict[str, Any] | None, active: bool | None, updated_by: str, now: str) -> dict[str, Any] | None:
        row = self.type_def(code)
        if row is None:
            return None
        fields: list[str] = []
        values: list[Any] = []
        if name is not None:
            fields.append("name=?")
            values.append(name)
        if description is not None:
            fields.append("description=?")
            values.append(description)
        if extra_schema is not None:
            fields.append("extra_schema_json=?")
            values.append(json.dumps(extra_schema, ensure_ascii=False, sort_keys=True))
        if active is not None:
            fields.append("active=?")
            values.append(1 if active else 0)
        if not fields:
            return dict(row)
        fields.append("updated_at=?")
        values.append(now)
        values.append(row["id"])
        self.connection.execute(f"UPDATE event_type_defs SET {','.join(fields)} WHERE id=?", values)
        return dict(self.type_def(code))

    def type_def_by_id(self, type_id: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM event_type_defs WHERE id=?", (type_id,)).fetchone()

    # ---- 事件 ----

    def next_seq(self) -> int:
        # 所有写入都在 BEGIN IMMEDIATE 事务内，MAX(seq)+1 不会并发重号
        row = self.connection.execute("SELECT COALESCE(MAX(seq),0)+1 FROM operational_events").fetchone()
        return int(row[0])

    def event_by_id(self, event_id: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM operational_events WHERE id=?", (event_id,)).fetchone()

    def event_by_seq(self, seq: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM operational_events WHERE seq=?", (seq,)).fetchone()

    def event_by_idempotency(self, source: str, key: str) -> sqlite3.Row | None:
        return self.connection.execute(
            "SELECT * FROM operational_events WHERE source=? AND idempotency_key=?", (source, key)
        ).fetchone()

    def create_event(self, *, seq: int, fields: dict[str, Any], now: str) -> int:
        columns = [
            "seq", "event_type", "satellite_code", "mission_code", "source", "severity",
            "title", "description", "occurred_at", "received_at", "updated_at", "extra_json",
            "idempotency_key", "payload_digest", "dedup_window_seconds", "root_event_id",
        ]
        values = [
            seq, fields["event_type"], fields["satellite_code"], fields["mission_code"], fields["source"],
            fields["severity"], fields["title"], fields["description"],
            fields["occurred_at"], now, now, json.dumps(fields["extra"], ensure_ascii=False, sort_keys=True),
            fields["idempotency_key"], fields["payload_digest"], fields["dedup_window_seconds"],
            fields.get("root_event_id"),
        ]
        cursor = self.connection.execute(
            f"INSERT INTO operational_events({','.join(columns)}) VALUES({','.join('?' for _ in columns)})",
            values,
        )
        return int(cursor.lastrowid)

    def find_duplicate(self, *, event_type: str, satellite_code: str, occurred_at: str, window_start: str, source: str, extra_digest: str) -> sqlite3.Row | None:
        # 去重键（无显式幂等键时）：来源+类型+卫星+时间窗+载荷指纹
        return self.connection.execute(
            "SELECT * FROM operational_events WHERE event_type=? AND satellite_code=? AND source=? "
            "AND occurred_at BETWEEN ? AND ? AND payload_digest=? AND status<>'archived' "
            "AND idempotency_key='' ORDER BY id DESC LIMIT 1",
            (event_type, satellite_code, source, window_start, occurred_at, extra_digest),
        ).fetchone()

    def update_event_fields(self, event_id: int, updates: dict[str, Any], now: str) -> None:
        if not updates:
            return
        clauses = [f"{key}=?" for key in updates]
        self.connection.execute(
            f"UPDATE operational_events SET {','.join(clauses)},updated_at=?,version=version+1 WHERE id=?",
            list(updates.values()) + [now, event_id],
        )

    def mark_status(self, event_id: int, status: str, actor: str, now: str, *, level: int | None = None) -> None:
        actor_columns = {
            "acknowledged": ("acknowledged_by", "acknowledged_at"),
            "escalated": ("escalated_by", "escalated_at"),
            "resolved": ("resolved_by", "resolved_at"),
            "closed": ("closed_by", "closed_at"),
            "archived": ("archived_by", "archived_at"),
        }
        sets = ["status=?", "updated_at=?", "version=version+1"]
        params: list[Any] = [status, now]
        if status in actor_columns:
            by_column, at_column = actor_columns[status]
            sets.append(f"{by_column}=?")
            params.append(actor)
            sets.append(f"{at_column}=?")
            params.append(now)
        if level is not None:
            sets.append("escalation_level=?")
            params.append(level)
        params.append(event_id)
        self.connection.execute(f"UPDATE operational_events SET {','.join(sets)} WHERE id=?", params)

    def hard_delete(self, event_id: int) -> None:
        self.connection.execute("DELETE FROM operational_events WHERE id=?", (event_id,))

    # ---- 游标分页：以 seq 为稳定全局游标，新写入不会造成重复或漏项 ----

    def list_events(
        self,
        *,
        after_seq: int | None,
        before_seq: int | None,
        limit: int,
        direction: str,
        event_type: str | None = None,
        satellite_code: str | None = None,
        mission_code: str | None = None,
        status: str | None = None,
        severity: str | None = None,
        source: str | None = None,
        occurred_from: str | None = None,
        occurred_to: str | None = None,
        statuses: Iterable[str] | None = None,
    ) -> list[sqlite3.Row]:
        clauses: list[str] = []
        values: list[Any] = []
        if after_seq is not None:
            clauses.append("seq>?")
            values.append(after_seq)
        if before_seq is not None:
            clauses.append("seq<?")
            values.append(before_seq)
        if event_type:
            clauses.append("event_type=?")
            values.append(event_type)
        if satellite_code:
            clauses.append("satellite_code=?")
            values.append(satellite_code)
        if mission_code:
            clauses.append("mission_code=?")
            values.append(mission_code)
        if status:
            clauses.append("status=?")
            values.append(status)
        if statuses:
            status_list = list(statuses)
            if status_list:
                clauses.append(f"status IN ({','.join('?' for _ in status_list)})")
                values.extend(status_list)
        if severity:
            clauses.append("severity=?")
            values.append(severity)
        if source:
            clauses.append("source=?")
            values.append(source)
        if occurred_from:
            clauses.append("occurred_at>=?")
            values.append(occurred_from)
        if occurred_to:
            clauses.append("occurred_at<=?")
            values.append(occurred_to)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        order = "ASC" if direction == "forward" else "DESC"
        values.append(limit + 1)
        rows = self.connection.execute(
            f"SELECT * FROM operational_events{where} ORDER BY seq {order} LIMIT ?", values
        ).fetchall()
        return rows

    def events_for_archive(self, *, statuses: list[str], occurred_before: str | None, satellite_code: str | None, mission_code: str | None, limit: int) -> list[sqlite3.Row]:
        clauses = [f"status IN ({','.join('?' for _ in statuses)})"]
        values: list[Any] = list(statuses)
        if occurred_before:
            clauses.append("occurred_at<?")
            values.append(occurred_before)
        if satellite_code:
            clauses.append("satellite_code=?")
            values.append(satellite_code)
        if mission_code:
            clauses.append("mission_code=?")
            values.append(mission_code)
        values.append(limit)
        return self.connection.execute(
            f"SELECT * FROM operational_events WHERE {' AND '.join(clauses)} ORDER BY seq LIMIT ?", values
        ).fetchall()

    # ---- 关联对象 ----

    def add_subject(self, *, event_id: int, object_type: str, object_key: str, role: str, label: str, now: str) -> None:
        self.connection.execute(
            "INSERT OR IGNORE INTO event_subjects(event_id,object_type,object_key,role,label,created_at) VALUES(?,?,?,?,?,?)",
            (event_id, object_type, object_key, role, label, now),
        )

    def subjects_for(self, event_ids: Iterable[int]) -> dict[int, list[dict[str, Any]]]:
        ids = list(event_ids)
        if not ids:
            return {}
        placeholders = ",".join("?" for _ in ids)
        rows = self.connection.execute(
            f"SELECT * FROM event_subjects WHERE event_id IN ({placeholders}) ORDER BY id", ids
        ).fetchall()
        result: dict[int, list[dict[str, Any]]] = {event_id: [] for event_id in ids}
        for row in rows:
            result.setdefault(int(row["event_id"]), []).append(dict(row))
        return result

    # ---- 关联关系 ----

    def add_link(self, *, event_id: int, linked_event_id: int, relation: str, note: str, created_by: str, now: str) -> bool:
        # 关联为有向边（如 caused_by/blocks/succeeds），查询时双向遍历并标注方向
        cursor = self.connection.execute(
            "INSERT OR IGNORE INTO event_links(event_id,linked_event_id,relation,note,created_by,created_at) VALUES(?,?,?,?,?,?)",
            (event_id, linked_event_id, relation, note, created_by, now),
        )
        return cursor.rowcount > 0

    def remove_link(self, event_id: int, linked_event_id: int, relation: str) -> None:
        self.connection.execute(
            "DELETE FROM event_links WHERE event_id=? AND linked_event_id=? AND relation=?",
            (event_id, linked_event_id, relation),
        )

    def links_for(self, event_id: int) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT l.*,e.seq AS other_seq,e.event_type AS other_event_type,e.title AS other_title,e.status AS other_status, "
            "CASE WHEN l.event_id=? THEN 'outgoing' ELSE 'incoming' END AS direction "
            "FROM event_links l JOIN operational_events e ON e.id=CASE WHEN l.event_id=? THEN l.linked_event_id ELSE l.event_id END "
            "WHERE l.event_id=? OR l.linked_event_id=? ORDER BY l.id",
            (event_id, event_id, event_id, event_id),
        ).fetchall()
        return [dict(row) for row in rows]

    # ---- 变更日志 ----

    def add_change(self, *, event_id: int | None, seq: int | None, action: str, actor: str, reason: str, before: Any, after: Any, batch_key: str, now: str) -> None:
        self.connection.execute(
            "INSERT INTO event_change_logs(event_id,seq,action,actor,reason,before_json,after_json,batch_key,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (
                event_id, seq, action, actor, reason,
                None if before is None else json.dumps(before, ensure_ascii=False, sort_keys=True),
                None if after is None else json.dumps(after, ensure_ascii=False, sort_keys=True),
                batch_key, now,
            ),
        )

    def changes_for(self, event_id: int) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute(
            "SELECT * FROM event_change_logs WHERE event_id=? ORDER BY id", (event_id,)
        ).fetchall()]

    def record_archive_batch(self, *, batch_key: str, criteria: dict[str, Any], amount: int, archived_by: str, now: str) -> None:
        self.connection.execute(
            "INSERT INTO event_archives(batch_key,criteria_json,amount,archived_by,created_at) VALUES(?,?,?,?,?)",
            (batch_key, json.dumps(criteria, ensure_ascii=False, sort_keys=True), amount, archived_by, now),
        )

    # ---- 聚合摘要 ----

    def satellite_summary(self, satellite_code: str) -> dict[str, Any]:
        row = self.connection.execute(
            "SELECT COUNT(*) AS total,MIN(occurred_at) AS first_at,MAX(occurred_at) AS last_at FROM operational_events WHERE satellite_code=?",
            (satellite_code,),
        ).fetchone()
        status_rows = self.connection.execute(
            "SELECT status,COUNT(*) AS amount FROM operational_events WHERE satellite_code=? GROUP BY status",
            (satellite_code,),
        ).fetchall()
        type_rows = self.connection.execute(
            "SELECT event_type,COUNT(*) AS amount FROM operational_events WHERE satellite_code=? GROUP BY event_type ORDER BY amount DESC",
            (satellite_code,),
        ).fetchall()
        open_alerts = self.connection.execute(
            "SELECT seq,event_type,severity,status,title,occurred_at,escalation_level FROM operational_events "
            "WHERE satellite_code=? AND status IN ('open','acknowledged','escalated') "
            "AND severity IN ('warning','critical') ORDER BY occurred_at DESC,seq DESC LIMIT 20",
            (satellite_code,),
        ).fetchall()
        return {
            "satellite_code": satellite_code,
            "total_events": int(row["total"]),
            "first_occurred_at": row["first_at"],
            "last_occurred_at": row["last_at"],
            "by_status": {r["status"]: int(r["amount"]) for r in status_rows},
            "by_type": {r["event_type"]: int(r["amount"]) for r in type_rows},
            "open_alerts": [dict(r) for r in open_alerts],
        }

    def mission_summary(self, mission_code: str) -> dict[str, Any]:
        row = self.connection.execute(
            "SELECT COUNT(*) AS total,COUNT(DISTINCT satellite_code) AS satellites,MIN(occurred_at) AS first_at,MAX(occurred_at) AS last_at FROM operational_events WHERE mission_code=?",
            (mission_code,),
        ).fetchone()
        status_rows = self.connection.execute(
            "SELECT status,COUNT(*) AS amount FROM operational_events WHERE mission_code=? GROUP BY status",
            (mission_code,),
        ).fetchall()
        severity_rows = self.connection.execute(
            "SELECT severity,COUNT(*) AS amount FROM operational_events WHERE mission_code=? GROUP BY severity",
            (mission_code,),
        ).fetchall()
        timeline = self.connection.execute(
            "SELECT seq,event_type,satellite_code,severity,status,title,occurred_at FROM operational_events WHERE mission_code=? ORDER BY occurred_at,seq",
            (mission_code,),
        ).fetchall()
        return {
            "mission_code": mission_code,
            "total_events": int(row["total"]),
            "satellite_count": int(row["satellites"]),
            "first_occurred_at": row["first_at"],
            "last_occurred_at": row["last_at"],
            "by_status": {r["status"]: int(r["amount"]) for r in status_rows},
            "by_severity": {r["severity"]: int(r["amount"]) for r in severity_rows},
            "timeline": [dict(r) for r in timeline],
        }
