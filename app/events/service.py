from __future__ import annotations

import hashlib
import json
import sqlite3
from typing import Any

from app.core.clock import Clock, SystemClock, to_storage
from app.core.errors import ConflictError, DomainError, NotFoundError, ValidationError
from app.database import get_connection, transaction
from app.events.cursor import decode_cursor, encode_cursor, filter_fingerprint
from app.events.repository import EventRepository

ACTIVE_TRANSITIONS = {
    "acknowledge": {"open", "escalated"},
    "escalate": {"open", "acknowledged"},
    "close": {"open", "acknowledged", "escalated"},
}
ARCHIVE_FROM = {"closed"}
ALLOWED_STATUSES = {"open", "acknowledged", "escalated", "closed", "archived"}


def digest(value: Any) -> str:
    text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()


class EventCenterService:
    """接收地面运营事件，维护时间线、处置流转、关联与运行摘要。"""

    def __init__(self, connection: sqlite3.Connection | None = None, clock: Clock | None = None) -> None:
        self.connection = connection or get_connection()
        self.clock = clock or SystemClock()
        self.repository = EventRepository(self.connection)

    # ---- 扩展字段注册表 ----

    def list_field_definitions(self) -> list[dict[str, Any]]:
        return [self._decode_definition(row) for row in self.repository.field_definitions()]

    def create_field_definition(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = EventRepository(connection)
            if repository.field_definition(payload["field_key"]):
                raise ConflictError("扩展字段已存在")
            row = repository.create_field_definition(
                field_key=payload["field_key"], value_type=payload["value_type"],
                allowed_values=payload["allowed_values"], required=payload["required"],
                applies_to=payload["applies_to"], description=payload["description"],
                created_by=actor, now=now,
            )
        return self._decode_definition(row)

    # ---- 关联对象 ----

    def upsert_object(self, payload: dict[str, Any]) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            row = EventRepository(connection).upsert_object(
                object_type=payload["object_type"], object_key=payload["object_key"],
                display_name=payload.get("display_name", ""), metadata=payload.get("metadata", {}), now=now,
            )
        return self._decode_object(row)

    def list_objects(self, object_type: str | None = None) -> list[dict[str, Any]]:
        return [self._decode_object(row) for row in self.repository.list_objects(object_type)]

    # ---- 事件接入与去重 ----

    def ingest(self, payload: dict[str, Any]) -> tuple[dict[str, Any], bool]:
        """写入单个事件；相同来源与外部编号视为重复，返回既有事件且 created=False。"""
        return self._ingest_one(payload)

    def ingest_batch(self, items: list[dict[str, Any]]) -> dict[str, Any]:
        created: list[dict[str, Any]] = []
        duplicates: list[dict[str, Any]] = []
        rejected: list[dict[str, Any]] = []
        for item in items:
            try:
                event, was_created = self._ingest_one(item)
            except DomainError as exc:
                rejected.append({
                    "source": item.get("source"), "external_id": item.get("external_id"),
                    "code": exc.code, "message": exc.message,
                })
                continue
            entry = {"id": event["id"], "source": event["source"], "external_id": event["external_id"]}
            (created if was_created else duplicates).append(entry)
        return {
            "created": created, "duplicates": duplicates, "rejected": rejected,
            "created_count": len(created), "duplicate_count": len(duplicates), "rejected_count": len(rejected),
        }

    def _ingest_one(self, payload: dict[str, Any]) -> tuple[dict[str, Any], bool]:
        now = to_storage(self.clock.now())
        occurred_at = to_storage(payload["occurred_at"])
        with transaction(immediate=True) as connection:
            repository = EventRepository(connection)
            existing = repository.event_by_external(payload["source"], payload["external_id"])
            if existing is not None:
                if existing["deleted_at"] is not None:
                    raise ConflictError("同编号事件已被删除，请先恢复再重新接入")
                return self._decorate(connection, dict(existing)), False
            attributes = self._validate_attributes(connection, payload["event_type"], payload["attributes"])
            satellite_id = self._resolve_object_id(connection, "satellite", payload.get("satellite"))
            mission_id = self._resolve_object_id(connection, "mission", payload.get("mission"))
            fingerprint = digest({k: payload[k] for k in ("source", "external_id", "event_type", "severity", "summary")})
            event = repository.create_event(
                source=payload["source"], external_id=payload["external_id"],
                event_type=payload["event_type"], severity=payload["severity"],
                satellite_id=satellite_id, mission_id=mission_id, occurred_at=occurred_at,
                summary=payload["summary"], attributes=attributes,
                correlation_key=payload.get("correlation_key", ""), payload_digest=fingerprint, now=now,
            )
            return self._decorate(connection, event), True

    # ---- 时间线与游标分页 ----

    def timeline(
        self,
        *,
        limit: int = 50,
        cursor_token: str | None = None,
        sort: str = "occurred",
        satellite: str | None = None,
        mission: str | None = None,
        event_type: str | None = None,
        severity: str | None = None,
        statuses: list[str] | None = None,
        source: str | None = None,
        correlation_key: str | None = None,
        include_deleted: bool = False,
    ) -> dict[str, Any]:
        limit = max(1, min(limit, 200))
        if statuses:
            invalid = sorted(set(statuses) - ALLOWED_STATUSES)
            if invalid:
                raise ValidationError("存在不支持的事件状态", context={"statuses": invalid})
        filters = {
            "satellite": satellite, "mission": mission, "event_type": event_type,
            "severity": severity, "statuses": sorted(statuses) if statuses else None,
            "source": source, "correlation_key": correlation_key, "include_deleted": include_deleted,
        }
        fingerprint = filter_fingerprint(filters)
        clauses: list[str] = []
        params: list[Any] = []
        if satellite:
            clauses.append("satellite_id IN (SELECT id FROM event_objects WHERE object_type=? AND object_key=?)")
            params.extend(["satellite", satellite])
        if mission:
            clauses.append("mission_id IN (SELECT id FROM event_objects WHERE object_type=? AND object_key=?)")
            params.extend(["mission", mission])
        if event_type:
            clauses.append("event_type=?")
            params.append(event_type)
        if severity:
            clauses.append("severity=?")
            params.append(severity)
        if statuses:
            clauses.append(f"status IN ({','.join('?' for _ in statuses)})")
            params.extend(statuses)
        if source:
            clauses.append("source=?")
            params.append(source)
        if correlation_key:
            clauses.append("correlation_key=?")
            params.append(correlation_key)
        if not include_deleted:
            clauses.append("deleted_at IS NULL")

        if cursor_token:
            position = decode_cursor(cursor_token, expected_sort=sort, expected_fingerprint=fingerprint)
            last_id = position["last_id"]
            if sort == "occurred":
                last_occurred = position["last_occurred_at"]
                if not last_occurred:
                    raise ValidationError("游标缺少排序位置")
                clauses.append("(occurred_at < ? OR (occurred_at = ? AND id < ?))")
                params.extend([last_occurred, last_occurred, last_id])
            else:
                clauses.append("id < ?")
                params.append(last_id)

        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        order = "occurred_at DESC, id DESC" if sort == "occurred" else "id DESC"
        rows = self.repository.list_events(where=where, params=params, order=order, limit=limit + 1)
        has_more = len(rows) > limit
        page = [self._decorate(self.connection, row) for row in rows[:limit]]
        next_cursor = ""
        if page and has_more:
            last = page[-1]
            next_cursor = encode_cursor(
                sort=sort, fingerprint=fingerprint, last_id=last["id"],
                last_occurred_at=last["occurred_at"] if sort == "occurred" else None,
            )
        return {"items": page, "next_cursor": next_cursor, "has_more": has_more, "limit": limit, "sort": sort}

    def get_event(self, event_id: int, *, include_deleted: bool = False) -> dict[str, Any]:
        row = self.repository.event_by_id(event_id)
        if row is None or (row["deleted_at"] is not None and not include_deleted):
            raise NotFoundError("运营事件不存在")
        return self._decorate(self.connection, dict(row), with_relations=True)

    # ---- 确认、升级、关闭、归档、删除、更正 ----

    def acknowledge(self, event_id: int, actor: str, reason: str) -> dict[str, Any]:
        return self._transition(event_id, actor, reason, "acknowledge", "acknowledged")

    def escalate(self, event_id: int, actor: str, reason: str) -> dict[str, Any]:
        return self._transition(event_id, actor, reason, "escalate", "escalated")

    def close(self, event_id: int, actor: str, resolution: str) -> dict[str, Any]:
        return self._transition(event_id, actor, resolution, "close", "closed")

    def archive(self, event_id: int, actor: str, reason: str, batch_key: str = "") -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = EventRepository(connection)
            row = self._require_mutable_event(repository, event_id)
            if row["status"] not in ARCHIVE_FROM:
                raise ConflictError("只有已关闭的事件可以归档")
            before = self._decorate(connection, dict(row))
            repository.update_event(event_id, {"status": "archived", "archived_at": now}, now)
            after = self._decorate(connection, dict(repository.event_by_id(event_id)))
            repository.add_journal(event_id=event_id, action="archive", actor=actor, reason=reason, before=before, after=after, batch_key=batch_key, now=now)
            return after

    def batch_archive(self, event_ids: list[int], actor: str, reason: str) -> dict[str, Any]:
        batch_key = digest({"actor": actor, "event_ids": sorted(set(event_ids)), "operation": "archive"})
        succeeded: list[int] = []
        failed: list[dict[str, Any]] = []
        for event_id in dict.fromkeys(event_ids):
            try:
                self.archive(event_id, actor, reason, batch_key)
                succeeded.append(event_id)
            except (ConflictError, NotFoundError) as exc:
                failed.append({"event_id": event_id, "code": exc.code, "message": exc.message})
        return {"batch_key": batch_key, "succeeded": succeeded, "failed": failed}

    def correct(self, event_id: int, actor: str, payload: dict[str, Any]) -> dict[str, Any]:
        """更正事件字段；payload 只包含调用方显式提供的字段，前后值都写入日志。"""
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = EventRepository(connection)
            row = self._require_mutable_event(repository, event_id)
            current = dict(row)
            changes: dict[str, Any] = {}
            before_values: dict[str, Any] = {}
            after_values: dict[str, Any] = {}

            def change(column: str, before: Any, after: Any) -> None:
                before_values[column] = before
                after_values[column] = after
                changes[column] = after

            for field in ("summary", "severity", "event_type"):
                value = payload.get(field)
                if value is not None and value != current[field]:
                    change(field, current[field], value)
            if "correlation_key" in payload and payload["correlation_key"] is not None:
                value = payload["correlation_key"]
                if value != current["correlation_key"]:
                    change("correlation_key", current["correlation_key"], value)
            if payload.get("occurred_at") is not None:
                value = to_storage(payload["occurred_at"])
                if value != current["occurred_at"]:
                    change("occurred_at", current["occurred_at"], value)
            if "satellite" in payload:
                new_id = self._resolve_object_id(connection, "satellite", payload["satellite"] or None)
                if new_id != current["satellite_id"]:
                    before_values["satellite"] = self._object_ref(connection, current["satellite_id"])
                    after_values["satellite"] = self._object_ref(connection, new_id)
                    changes["satellite_id"] = new_id
            if "mission" in payload:
                new_id = self._resolve_object_id(connection, "mission", payload["mission"] or None)
                if new_id != current["mission_id"]:
                    before_values["mission"] = self._object_ref(connection, current["mission_id"])
                    after_values["mission"] = self._object_ref(connection, new_id)
                    changes["mission_id"] = new_id
            if payload.get("attributes") is not None:
                event_type = after_values.get("event_type", current["event_type"])
                attributes = self._validate_attributes(connection, event_type, payload["attributes"])
                encoded = json.dumps(attributes, ensure_ascii=False, sort_keys=True)
                if encoded != current["attributes_json"]:
                    before_values["attributes"] = json.loads(current["attributes_json"])
                    after_values["attributes"] = attributes
                    changes["attributes_json"] = encoded

            if not changes:
                raise ConflictError("没有需要更正的字段或更正内容与当前值相同")
            before_snapshot = self._decorate(connection, current)
            repository.update_event(event_id, changes, now)
            after_snapshot = self._decorate(connection, dict(repository.event_by_id(event_id)))
            repository.add_journal(
                event_id=event_id, action="correct", actor=actor, reason=payload["reason"],
                before={"changed": before_values, "snapshot": before_snapshot},
                after={"changed": after_values, "snapshot": after_snapshot},
                batch_key="", now=now,
            )
            return after_snapshot

    def soft_delete(self, event_id: int, actor: str, reason: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = EventRepository(connection)
            row = repository.event_by_id(event_id)
            if row is None:
                raise NotFoundError("运营事件不存在")
            if row["deleted_at"] is not None:
                raise ConflictError("事件已经处于删除状态")
            before = self._decorate(connection, dict(row))
            repository.update_event(event_id, {"deleted_at": now}, now)
            after = self._decorate(connection, dict(repository.event_by_id(event_id)))
            repository.add_journal(event_id=event_id, action="delete", actor=actor, reason=reason, before=before, after=after, batch_key="", now=now)
            return after

    def restore(self, event_id: int, actor: str, reason: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = EventRepository(connection)
            row = repository.event_by_id(event_id)
            if row is None:
                raise NotFoundError("运营事件不存在")
            if row["deleted_at"] is None:
                raise ConflictError("事件未被删除")
            before = self._decorate(connection, dict(row))
            repository.update_event(event_id, {"deleted_at": None}, now)
            after = self._decorate(connection, dict(repository.event_by_id(event_id)))
            repository.add_journal(event_id=event_id, action="restore", actor=actor, reason=reason, before=before, after=after, batch_key="", now=now)
            return after

    def journal(self, event_id: int) -> list[dict[str, Any]]:
        if self.repository.event_by_id(event_id) is None:
            raise NotFoundError("运营事件不存在")
        rows = self.repository.journal_for(event_id)
        for row in rows:
            row["before"] = json.loads(row.pop("before_json"))
            row["after"] = json.loads(row.pop("after_json"))
        return rows

    # ---- 事件关联 ----

    def link_events(self, event_id: int, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        related_id = payload["related_event_id"]
        if related_id == event_id:
            raise ValidationError("事件不能与自身建立关联")
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = EventRepository(connection)
            for candidate in (event_id, related_id):
                row = repository.event_by_id(candidate)
                if row is None or row["deleted_at"] is not None:
                    raise NotFoundError(f"关联事件不存在：{candidate}")
            try:
                repository.add_link(event_id=event_id, related_event_id=related_id, link_type=payload["link_type"], created_by=actor, now=now)
            except sqlite3.IntegrityError as exc:
                raise ConflictError("两个事件之间已存在相同类型的关联") from exc
        return {"event_id": event_id, "related_event_id": related_id, "link_type": payload["link_type"]}

    # ---- 命名游标与历史回放 ----

    def create_playback_cursor(self, name: str, from_event_id: int, actor: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            if from_event_id and EventRepository(connection).event_by_id(from_event_id) is None:
                raise NotFoundError("起始事件不存在")
            return dict(EventRepository(connection).create_cursor(name=name, last_event_id=from_event_id, created_by=actor, now=now))

    def get_playback_cursor(self, name: str) -> dict[str, Any]:
        row = self.repository.cursor_by_name(name)
        if row is None:
            raise NotFoundError("回放游标不存在")
        return dict(row)

    def list_playback_cursors(self) -> list[dict[str, Any]]:
        return self.repository.list_cursors()

    def replay(self, name: str, limit: int = 100) -> dict[str, Any]:
        """按命名游标顺序回放新事件，并把游标单调推进到本次最后一条。"""
        limit = max(1, min(limit, 500))
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = EventRepository(connection)
            row = repository.cursor_by_name(name)
            if row is None:
                raise NotFoundError("回放游标不存在")
            expected_last = int(row["last_event_id"])
            events = [self._decorate(connection, item) for item in repository.stream_after(last_event_id=expected_last, limit=limit)]
            new_last = events[-1]["id"] if events else expected_last
            if events and not repository.advance_cursor(name=name, expected_last=expected_last, new_last=new_last, now=now):
                raise ConflictError("回放游标已被其他进程推进，请重试")
            return {
                "cursor": name,
                "last_event_id": new_last,
                "advanced": new_last != expected_last,
                "items": events,
                "has_more": len(events) == limit,
            }

    def replay_from(self, after_event_id: int, limit: int = 100) -> dict[str, Any]:
        """无状态历史回放：按接入顺序返回编号大于 after_event_id 的事件。"""
        limit = max(1, min(limit, 500))
        rows = self.repository.stream_after(last_event_id=after_event_id, limit=limit)
        items = [self._decorate(self.connection, row) for row in rows]
        return {
            "last_event_id": items[-1]["id"] if items else after_event_id,
            "items": items,
            "has_more": len(items) == limit,
        }

    def delete_playback_cursor(self, name: str) -> dict[str, Any]:
        if not self.repository.delete_cursor(name):
            raise NotFoundError("回放游标不存在")
        return {"deleted": name}

    # ---- 运行摘要 ----

    def overview(self) -> dict[str, Any]:
        rows = self.connection.execute(
            "SELECT status,severity,COUNT(*) AS amount FROM ops_events WHERE deleted_at IS NULL GROUP BY status,severity"
        ).fetchall()
        status_counts: dict[str, int] = {}
        critical_active = 0
        for row in rows:
            status_counts[row["status"]] = status_counts.get(row["status"], 0) + int(row["amount"])
            if row["status"] in {"open", "acknowledged", "escalated"} and row["severity"] == "critical":
                critical_active += int(row["amount"])
        recent = self.connection.execute(
            "SELECT id FROM ops_events WHERE deleted_at IS NULL ORDER BY occurred_at DESC,id DESC LIMIT 10"
        ).fetchall()
        return {
            "status_counts": status_counts,
            "active_count": sum(status_counts.get(s, 0) for s in ("open", "acknowledged", "escalated")),
            "critical_active_count": critical_active,
            "recent_event_ids": [row["id"] for row in recent],
        }

    def summary(self, dimension: str) -> dict[str, Any]:
        if dimension not in {"satellite", "mission"}:
            raise ValidationError("聚合维度只能是 satellite 或 mission")
        group_column = "satellite_id" if dimension == "satellite" else "mission_id"
        groups: dict[int, dict[str, Any]] = {}
        for row in self.repository.summary_rows(group_column):
            group_id = row["group_id"]
            if group_id is None:
                continue
            group = groups.setdefault(group_id, {
                "object": None, "total": 0, "by_status": {}, "by_severity": {}, "by_type": {},
                "active_count": 0, "critical_active_count": 0, "last_occurred_at": None,
            })
            amount = int(row["amount"])
            group["total"] += amount
            group["by_status"][row["status"]] = group["by_status"].get(row["status"], 0) + amount
            group["by_severity"][row["severity"]] = group["by_severity"].get(row["severity"], 0) + amount
            group["by_type"][row["event_type"]] = group["by_type"].get(row["event_type"], 0) + amount
            if row["status"] in ("open", "acknowledged", "escalated"):
                group["active_count"] += amount
                if row["severity"] == "critical":
                    group["critical_active_count"] += amount
            if group["last_occurred_at"] is None or (row["last_occurred_at"] or "") > group["last_occurred_at"]:
                group["last_occurred_at"] = row["last_occurred_at"]
        objects = {row["id"]: row for row in self.list_objects()}
        result = []
        for group_id, group in groups.items():
            group["object"] = objects.get(group_id)
            last = self.repository.last_event_for(group_column, group_id)
            group["last_event"] = dict(last) if last else None
            result.append(group)
        result.sort(key=lambda item: (item["object"] or {}).get("object_key", ""))
        return {"dimension": dimension, "groups": result}

    # ---- 内部辅助 ----

    def _transition(self, event_id: int, actor: str, reason: str, action: str, target_status: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = EventRepository(connection)
            row = self._require_mutable_event(repository, event_id)
            if row["status"] not in ACTIVE_TRANSITIONS[action]:
                raise ConflictError(f"事件当前状态 {row['status']} 不允许执行 {action}")
            before = self._decorate(connection, dict(row))
            repository.update_event(event_id, {"status": target_status}, now)
            after = self._decorate(connection, dict(repository.event_by_id(event_id)))
            repository.add_journal(event_id=event_id, action=action, actor=actor, reason=reason, before=before, after=after, batch_key="", now=now)
            return after

    @staticmethod
    def _require_mutable_event(repository: EventRepository, event_id: int) -> sqlite3.Row:
        row = repository.event_by_id(event_id)
        if row is None:
            raise NotFoundError("运营事件不存在")
        if row["deleted_at"] is not None:
            raise ConflictError("事件已删除，请先恢复")
        if row["status"] == "archived":
            raise ConflictError("事件已归档，不能再变更")
        return row

    @staticmethod
    def _resolve_object_id(connection: sqlite3.Connection, object_type: str, key: str | None) -> int | None:
        if not key:
            return None
        row = EventRepository(connection).object_by_key(object_type, key)
        if row is None:
            raise NotFoundError(f"关联对象不存在，请先登记：{object_type}/{key}")
        return int(row["id"])

    def _validate_attributes(self, connection: sqlite3.Connection, event_type: str, attributes: dict[str, Any]) -> dict[str, Any]:
        definitions = EventRepository(connection).field_definitions()
        registry = {row["field_key"]: row for row in definitions}
        unknown = sorted(set(attributes) - set(registry))
        if unknown:
            raise ValidationError("包含未注册的扩展字段", context={"fields": unknown})
        normalized: dict[str, Any] = {}
        for field_key, row in registry.items():
            applies_to = json.loads(row["applies_to_json"])
            if applies_to and event_type not in applies_to:
                if field_key in attributes:
                    raise ValidationError(f"扩展字段 {field_key} 不适用于事件类型 {event_type}")
                continue
            if field_key not in attributes:
                if row["required"]:
                    raise ValidationError(f"缺少必填扩展字段：{field_key}")
                continue
            value = attributes[field_key]
            value_type = row["value_type"]
            valid = {
                "string": isinstance(value, str),
                "integer": isinstance(value, int) and not isinstance(value, bool),
                "number": isinstance(value, (int, float)) and not isinstance(value, bool),
                "boolean": isinstance(value, bool),
                "enum": isinstance(value, str),
            }[value_type]
            if not valid:
                raise ValidationError(f"扩展字段 {field_key} 类型不正确，应为 {value_type}")
            if value_type == "enum":
                allowed = json.loads(row["allowed_values_json"])
                if value not in allowed:
                    raise ValidationError(f"扩展字段 {field_key} 不在允许取值内", context={"allowed": allowed})
            normalized[field_key] = value
        return normalized

    def _decorate(self, connection: sqlite3.Connection, row: dict[str, Any], *, with_relations: bool = False) -> dict[str, Any]:
        event = dict(row)
        event["attributes"] = json.loads(event.pop("attributes_json"))
        event["satellite"] = self._object_ref(connection, event.get("satellite_id"))
        event["mission"] = self._object_ref(connection, event.get("mission_id"))
        if with_relations:
            repository = EventRepository(connection)
            event["links"] = repository.links_for(event["id"])
            event["correlated"] = repository.correlated_events(event["correlation_key"], exclude_id=event["id"]) if event["correlation_key"] else []
        return event

    @staticmethod
    def _object_ref(connection: sqlite3.Connection, object_id: int | None) -> dict[str, Any] | None:
        if not object_id:
            return None
        row = connection.execute("SELECT id,object_type,object_key,display_name FROM event_objects WHERE id=?", (object_id,)).fetchone()
        return dict(row) if row else None

    @staticmethod
    def _decode_definition(row: dict[str, Any]) -> dict[str, Any]:
        result = dict(row)
        result["allowed_values"] = json.loads(result.pop("allowed_values_json"))
        result["applies_to"] = json.loads(result.pop("applies_to_json"))
        result["required"] = bool(result["required"])
        return result

    @staticmethod
    def _decode_object(row: dict[str, Any]) -> dict[str, Any]:
        result = dict(row)
        result["metadata"] = json.loads(result.pop("metadata_json"))
        return result
