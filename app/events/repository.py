from __future__ import annotations

import json
import sqlite3
from typing import Any


class EventRepository:
    """封装事件中心领域的 SQLite 读写。"""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection

    # ---- 扩展字段注册表 ----

    def field_definitions(self) -> list[dict[str, Any]]:
        rows = self.connection.execute("SELECT * FROM event_field_definitions ORDER BY field_key").fetchall()
        return [dict(row) for row in rows]

    def field_definition(self, field_key: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM event_field_definitions WHERE field_key=?", (field_key,)).fetchone()

    def create_field_definition(self, *, field_key: str, value_type: str, allowed_values: list[str], required: bool, applies_to: list[str], description: str, created_by: str, now: str) -> dict[str, Any]:
        self.connection.execute(
            "INSERT INTO event_field_definitions(field_key,value_type,allowed_values_json,required,applies_to_json,description,created_by,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (field_key, value_type, json.dumps(allowed_values, ensure_ascii=False), 1 if required else 0, json.dumps(sorted(applies_to), ensure_ascii=False), description, created_by, now),
        )
        return dict(self.field_definition(field_key))

    # ---- 关联对象 ----

    def object_by_key(self, object_type: str, object_key: str) -> sqlite3.Row | None:
        return self.connection.execute(
            "SELECT * FROM event_objects WHERE object_type=? AND object_key=?", (object_type, object_key)
        ).fetchone()

    def upsert_object(self, *, object_type: str, object_key: str, display_name: str, metadata: dict[str, Any], now: str) -> dict[str, Any]:
        self.connection.execute(
            "INSERT INTO event_objects(object_type,object_key,display_name,metadata_json,created_at,updated_at) VALUES(?,?,?,?,?,?) "
            "ON CONFLICT(object_type,object_key) DO UPDATE SET display_name=excluded.display_name,metadata_json=excluded.metadata_json,updated_at=excluded.updated_at",
            (object_type, object_key, display_name or object_key, json.dumps(metadata, ensure_ascii=False, sort_keys=True), now, now),
        )
        return dict(self.object_by_key(object_type, object_key))

    def list_objects(self, object_type: str | None) -> list[dict[str, Any]]:
        if object_type:
            rows = self.connection.execute("SELECT * FROM event_objects WHERE object_type=? ORDER BY object_key", (object_type,)).fetchall()
        else:
            rows = self.connection.execute("SELECT * FROM event_objects ORDER BY object_type, object_key").fetchall()
        return [dict(row) for row in rows]

    # ---- 事件 ----

    def event_by_id(self, event_id: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM ops_events WHERE id=?", (event_id,)).fetchone()

    def event_by_external(self, source: str, external_id: str) -> sqlite3.Row | None:
        return self.connection.execute(
            "SELECT * FROM ops_events WHERE source=? AND external_id=?", (source, external_id)
        ).fetchone()

    def create_event(self, *, source: str, external_id: str, event_type: str, severity: str, satellite_id: int | None, mission_id: int | None, occurred_at: str, summary: str, attributes: dict[str, Any], correlation_key: str, payload_digest: str, now: str) -> dict[str, Any]:
        cursor = self.connection.execute(
            "INSERT INTO ops_events(source,external_id,event_type,severity,status,satellite_id,mission_id,occurred_at,summary,attributes_json,correlation_key,payload_digest,created_at,updated_at) "
            "VALUES(?,?,?,?,'open',?,?,?,?,?,?,?,?,?)",
            (source, external_id, event_type, severity, satellite_id, mission_id, occurred_at, summary, json.dumps(attributes, ensure_ascii=False, sort_keys=True), correlation_key, payload_digest, now, now),
        )
        return dict(self.event_by_id(cursor.lastrowid))

    def update_event(self, event_id: int, changes: dict[str, Any], now: str) -> None:
        assignments = ",".join(f"{column}=?" for column in changes)
        values = list(changes.values())
        values.extend([now, event_id])
        self.connection.execute(
            f"UPDATE ops_events SET {assignments},updated_at=?,version=version+1 WHERE id=?",
            values,
        )

    def list_events(self, *, where: str, params: list[Any], order: str, limit: int) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            f"SELECT * FROM ops_events {where} ORDER BY {order} LIMIT ?",
            (*params, limit),
        ).fetchall()
        return [dict(row) for row in rows]

    def stream_after(self, *, last_event_id: int, limit: int) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT * FROM ops_events WHERE id>? ORDER BY id ASC LIMIT ?",
            (last_event_id, limit),
        ).fetchall()
        return [dict(row) for row in rows]

    # ---- 事件关联 ----

    def add_link(self, *, event_id: int, related_event_id: int, link_type: str, created_by: str, now: str) -> None:
        self.connection.execute(
            "INSERT INTO ops_event_links(event_id,related_event_id,link_type,created_by,created_at) VALUES(?,?,?,?,?)",
            (event_id, related_event_id, link_type, created_by, now),
        )

    def links_for(self, event_id: int) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT * FROM ops_event_links WHERE event_id=? OR related_event_id=? ORDER BY id",
            (event_id, event_id),
        ).fetchall()
        return [dict(row) for row in rows]

    def correlated_events(self, correlation_key: str, *, exclude_id: int, limit: int = 20) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT id,event_type,severity,status,occurred_at,summary FROM ops_events WHERE correlation_key=? AND id<>? AND deleted_at IS NULL ORDER BY occurred_at,id LIMIT ?",
            (correlation_key, exclude_id, limit),
        ).fetchall()
        return [dict(row) for row in rows]

    # ---- 处置与审计日志 ----

    def add_journal(self, *, event_id: int, action: str, actor: str, reason: str, before: dict[str, Any], after: dict[str, Any], batch_key: str, now: str) -> None:
        self.connection.execute(
            "INSERT INTO ops_event_journal(event_id,action,actor,reason,before_json,after_json,batch_key,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (event_id, action, actor, reason, json.dumps(before, ensure_ascii=False, sort_keys=True), json.dumps(after, ensure_ascii=False, sort_keys=True), batch_key, now),
        )

    def journal_for(self, event_id: int) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT * FROM ops_event_journal WHERE event_id=? ORDER BY id", (event_id,)
        ).fetchall()
        return [dict(row) for row in rows]

    # ---- 命名游标 ----

    def cursor_by_name(self, name: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM ops_event_cursors WHERE name=?", (name,)).fetchone()

    def list_cursors(self) -> list[dict[str, Any]]:
        rows = self.connection.execute("SELECT * FROM ops_event_cursors ORDER BY name").fetchall()
        return [dict(row) for row in rows]

    def create_cursor(self, *, name: str, last_event_id: int, created_by: str, now: str) -> dict[str, Any]:
        self.connection.execute(
            "INSERT INTO ops_event_cursors(name,last_event_id,created_by,created_at,updated_at) VALUES(?,?,?,?,?) "
            "ON CONFLICT(name) DO UPDATE SET last_event_id=excluded.last_event_id,updated_at=excluded.updated_at",
            (name, last_event_id, created_by, now, now),
        )
        return dict(self.cursor_by_name(name))

    def advance_cursor(self, *, name: str, expected_last: int, new_last: int, now: str) -> bool:
        cursor = self.connection.execute(
            "UPDATE ops_event_cursors SET last_event_id=?,updated_at=? WHERE name=? AND last_event_id=?",
            (new_last, now, name, expected_last),
        )
        return cursor.rowcount == 1

    def delete_cursor(self, name: str) -> bool:
        cursor = self.connection.execute("DELETE FROM ops_event_cursors WHERE name=?", (name,))
        return cursor.rowcount == 1

    # ---- 聚合摘要 ----

    def summary_rows(self, group_column: str) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            f"SELECT {group_column} AS group_id, event_type, severity, status, COUNT(*) AS amount, MAX(occurred_at) AS last_occurred_at "
            f"FROM ops_events WHERE deleted_at IS NULL GROUP BY {group_column}, event_type, severity, status"
        ).fetchall()
        return [dict(row) for row in rows]

    def last_event_for(self, group_column: str, group_id: int | None) -> sqlite3.Row | None:
        condition = f"{group_column} IS NULL" if group_id is None else f"{group_column}=?"
        params: tuple = () if group_id is None else (group_id,)
        return self.connection.execute(
            f"SELECT id,summary,occurred_at,severity,event_type FROM ops_events WHERE deleted_at IS NULL AND {condition} ORDER BY occurred_at DESC, id DESC LIMIT 1",
            params,
        ).fetchone()
