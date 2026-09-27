from __future__ import annotations

import json
import sqlite3
from typing import Any

from app.core.clock import Clock, SystemClock, from_storage, to_storage
from app.core.errors import ConflictError, NotFoundError, ValidationError
from app.database import get_connection, transaction
from app.events.store import EventStore, ensure_schema


def canonical_digest(value: Any) -> str:
    import hashlib

    text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()


ACTIVE_STATUSES = {"open", "acknowledged", "escalated"}
TERMINAL_STATUSES = {"resolved", "closed"}


class EventCenterService:
    """接收多来源运营事件，提供去重、关联、流转、审计更正与聚合摘要。"""

    def __init__(self, connection: sqlite3.Connection | None = None, clock: Clock | None = None) -> None:
        self.connection = connection or get_connection()
        self.clock = clock or SystemClock()
        ensure_schema(self.connection)
        self.store = EventStore(self.connection)

    # ---- 受控类型词表 ----

    def init_builtins(self) -> None:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            EventStore(connection).seed_builtin_types(now)

    def list_types(self, *, include_inactive: bool = False) -> list[dict[str, Any]]:
        rows = self.store.list_type_defs(include_inactive=include_inactive)
        for row in rows:
            row["extra_schema"] = json.loads(row["extra_schema_json"] or "{}")
        return rows

    def create_type(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        self._validate_extra_schema(payload["extra_schema"])
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            store = EventStore(connection)
            if store.type_def(payload["code"]):
                raise ConflictError("事件类型编码已存在")
            result = store.create_type_def(
                code=payload["code"], name=payload["name"], category=payload["category"],
                lifecycle=payload["lifecycle"], description=payload["description"],
                extra_schema=payload["extra_schema"], created_by=actor, now=now,
            )
            result["extra_schema"] = json.loads(result["extra_schema_json"] or "{}")
            return result

    def update_type(self, code: str, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        if payload.get("extra_schema") is not None:
            self._validate_extra_schema(payload["extra_schema"])
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            store = EventStore(connection)
            existing = store.type_def(code)
            if existing is None:
                raise NotFoundError("事件类型不存在")
            result = store.update_type_def(
                code, name=payload.get("name"), description=payload.get("description"),
                extra_schema=payload.get("extra_schema"), active=payload.get("active"),
                updated_by=actor, now=now,
            )
            result["extra_schema"] = json.loads(result["extra_schema_json"] or "{}")
            return result

    # ---- 事件写入（幂等 + 去重） ----

    def ingest(self, payload: dict[str, Any]) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        occurred = self._normalize_occurred(payload.get("occurred_at"), now)
        with transaction(immediate=True) as connection:
            store = EventStore(connection)
            type_def = store.type_def(payload["event_type"])
            if type_def is None or not type_def["active"]:
                raise NotFoundError("事件类型不存在或已经停用")
            extra_schema = json.loads(type_def["extra_schema_json"] or "{}")
            extra = self._validate_extra(extra_schema, payload.get("extra", {}))
            core = {
                "event_type": payload["event_type"],
                "satellite_code": payload["satellite_code"].strip(),
                "mission_code": payload["mission_code"].strip(),
                "source": payload["source"].strip(),
                "severity": payload["severity"],
                "title": payload["title"].strip(),
                "description": payload["description"],
                "occurred_at": occurred,
                "extra": extra,
            }
            digest = canonical_digest({key: value for key, value in core.items() if key != "occurred_at"})
            key = payload["idempotency_key"].strip()

            existing: sqlite3.Row | None = None
            deduplicated = False
            if key:
                existing = store.event_by_idempotency(core["source"], key)
                if existing is not None:
                    if existing["payload_digest"] != digest:
                        raise ConflictError("同一来源与幂等键对应了不同的事件内容")
                    return {"event": self._serialize(store.event_by_id(existing["id"]), store), "replayed": True, "deduplicated": False}
            elif payload["dedup_window_seconds"] > 0:
                from datetime import timedelta

                window_start = to_storage(from_storage(occurred) - timedelta(seconds=payload["dedup_window_seconds"]))
                existing = store.find_duplicate(
                    event_type=core["event_type"], satellite_code=core["satellite_code"],
                    occurred_at=occurred, window_start=window_start, source=core["source"],
                    extra_digest=digest,
                )
                if existing is not None:
                    deduplicated = True

            if existing is not None:
                return {"event": self._serialize(store.event_by_id(existing["id"]), store), "replayed": False, "deduplicated": deduplicated}

            root_id = None
            if payload.get("root_event_seq") is not None:
                root = store.event_by_seq(int(payload["root_event_seq"]))
                if root is None:
                    raise NotFoundError("关联主线事件不存在")
                root_id = root["id"]

            seq = store.next_seq()
            fields = {
                **core,
                "idempotency_key": key,
                "payload_digest": digest,
                "dedup_window_seconds": payload["dedup_window_seconds"],
                "root_event_id": root_id,
            }
            event_id = store.create_event(seq=seq, fields=fields, now=now)
            for subject in payload.get("subjects", []):
                store.add_subject(
                    event_id=event_id, object_type=subject["object_type"], object_key=subject["object_key"],
                    role=subject["role"], label=subject["label"], now=now,
                )
            store.add_change(event_id=event_id, seq=seq, action="create", actor=core["source"], reason="事件接入", before=None, after=core, batch_key="", now=now)
            return {"event": self._serialize(store.event_by_id(event_id), store), "replayed": False, "deduplicated": False}

    # ---- 游标时间线 ----

    def timeline(
        self,
        *,
        limit: int,
        cursor_seq: int | None,
        direction: str,
        fingerprint: str,
        filters: dict[str, Any],
    ) -> dict[str, Any]:
        from app.events.cursor import encode_cursor

        store = self.store
        anchor = cursor_seq
        if direction == "backward" and anchor is None:
            anchor_row = store.connection.execute("SELECT COALESCE(MAX(seq),0)+1 FROM operational_events").fetchone()
            anchor = int(anchor_row[0])
        rows = store.list_events(
            after_seq=anchor if direction == "forward" else None,
            before_seq=anchor if direction == "backward" else None,
            limit=limit,
            direction=direction,
            **filters,
        )
        has_more = len(rows) > limit
        page_rows = rows[:limit]
        items = [self._serialize(row, store) for row in page_rows]
        result: dict[str, Any] = {
            "items": items,
            "page": {
                "limit": limit,
                "direction": direction,
                "has_more": has_more,
                "next_cursor": None,
            },
        }
        if page_rows:
            edge = page_rows[-1]
            result["page"]["next_cursor"] = encode_cursor(seq=int(edge["seq"]), fingerprint=fingerprint)
        return result

    def get_event(self, seq: int, *, with_links: bool = True) -> dict[str, Any]:
        row = self.store.event_by_seq(seq)
        if row is None:
            raise NotFoundError("事件不存在或已删除")
        result = self._serialize(row, self.store)
        if with_links:
            result["links"] = self.store.links_for(int(row["id"]))
        return result

    def changes(self, seq: int) -> list[dict[str, Any]]:
        # 日志始终带 seq；事件被硬删除后 event_id 置空，仍可凭 seq 查到墓碑与前后值
        rows = self.store.connection.execute(
            "SELECT * FROM event_change_logs WHERE seq=? ORDER BY id", (seq,)
        ).fetchall()
        if not rows:
            exists = self.store.event_by_seq(seq)
            if exists is None:
                raise NotFoundError("事件不存在或已删除")
        return [dict(row) for row in rows]

    # ---- 生命周期流转：确认 / 升级 / 解决 / 关闭 ----

    def acknowledge(self, seq: int, actor: str, note: str) -> dict[str, Any]:
        return self._transition(seq, actor, "acknowledge", note, allowed_from={"open"}, target="acknowledged")

    def escalate(self, seq: int, actor: str, reason: str, level: int) -> dict[str, Any]:
        def mutate(store: EventStore, event: sqlite3.Row, now: str) -> None:
            store.mark_status(int(event["id"]), "escalated", actor, now, level=level)

        return self._transition(seq, actor, "escalate", reason, {"open", "acknowledged"}, "escalated", mutate, extra_after={"escalation_level": level})

    def resolve(self, seq: int, actor: str, reason: str) -> dict[str, Any]:
        return self._transition(seq, actor, "resolve", reason, {"open", "acknowledged", "escalated"}, "resolved")

    def close(self, seq: int, actor: str, reason: str) -> dict[str, Any]:
        with transaction(immediate=True) as connection:
            store = EventStore(connection)
            event = self._require_by_seq(store, seq)
            type_def = store.type_def(event["event_type"])
            allowed = {"resolved"}
            if type_def is not None and type_def["lifecycle"] == "advisory":
                # 通告类事件不强制确认/解决流程
                allowed |= {"open", "acknowledged"}
            if event["status"] not in allowed:
                raise ConflictError(f"事件当前状态 {event['status']} 不允许关闭，需先解决")
            now = to_storage(self.clock.now())
            store.mark_status(int(event["id"]), "closed", actor, now)
            return self._apply_transition(store, event, "close", actor, reason, "closed", now=now)

    # ---- 更正与删除（保留前后值与操作者） ----

    def correct(self, seq: int, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            store = EventStore(connection)
            event = self._require_by_seq(store, seq)
            if event["status"] == "archived":
                raise ConflictError("已归档事件不能更正")
            before = self._serialize(event, store)
            updates: dict[str, Any] = {}
            for key in ("title", "description", "severity", "mission_code"):
                if payload.get(key) is not None:
                    value = payload[key]
                    updates[key] = value.strip() if isinstance(value, str) else value
            if payload.get("occurred_at") is not None:
                updates["occurred_at"] = self._normalize_occurred(payload["occurred_at"], now)
            new_type = event["event_type"]
            if payload.get("event_type"):
                type_def = store.type_def(payload["event_type"])
                if type_def is None or not type_def["active"]:
                    raise NotFoundError("事件类型不存在或已经停用")
                new_type = payload["event_type"]
                updates["event_type"] = new_type
            extra = before["extra"]
            if payload.get("extra") is not None:
                type_def = store.type_def(new_type)
                extra_schema = json.loads(type_def["extra_schema_json"] or "{}")
                extra = self._validate_extra(extra_schema, payload["extra"])
                updates["extra_json"] = json.dumps(extra, ensure_ascii=False, sort_keys=True)
            if updates:
                store.update_event_fields(int(event["id"]), updates, now)
            refreshed = store.event_by_id(int(event["id"]))
            after = self._serialize(refreshed, store)
            store.add_change(
                event_id=int(event["id"]), seq=int(event["seq"]), action="correct", actor=actor,
                reason=payload["reason"], before=before, after=after, batch_key="", now=now,
            )
            return after

    def delete(self, seq: int, reason: str, actor: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            store = EventStore(connection)
            event = self._require_by_seq(store, seq)
            before = self._serialize(event, store)
            # 先写日志（外键 event_id 仍有效），再硬删除；日志 ON DELETE SET NULL 并保留 seq
            store.add_change(
                event_id=int(event["id"]), seq=int(event["seq"]), action="delete", actor=actor,
                reason=reason, before=before, after=None, batch_key="", now=now,
            )
            store.hard_delete(int(event["id"]))
            return {"seq": int(event["seq"]), "deleted": True, "reason": reason, "actor": actor}

    # ---- 关联 ----

    def link(self, seq: int, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            store = EventStore(connection)
            event = self._require_by_seq(store, seq)
            other = self._require_by_seq(store, int(payload["linked_event_seq"]))
            if int(event["id"]) == int(other["id"]):
                raise ValidationError("事件不能与自身建立关联")
            inserted = store.add_link(
                event_id=int(event["id"]), linked_event_id=int(other["id"]),
                relation=payload["relation"], note=payload["note"], created_by=actor, now=now,
            )
            if not inserted:
                raise ConflictError("该关联关系已经存在")
            store.add_change(
                event_id=int(event["id"]), seq=int(event["seq"]), action="relate", actor=actor,
                reason=payload["note"] or payload["relation"],
                before=None, after={"linked_event_seq": int(other["seq"]), "relation": payload["relation"]},
                batch_key="", now=now,
            )
            return {"event_seq": seq, "links": store.links_for(int(event["id"]))}

    def unlink(self, seq: int, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            store = EventStore(connection)
            event = self._require_by_seq(store, seq)
            other = self._require_by_seq(store, int(payload["linked_event_seq"]))
            store.remove_link(int(event["id"]), int(other["id"]), payload["relation"])
            store.add_change(
                event_id=int(event["id"]), seq=int(event["seq"]), action="unrelate", actor=actor,
                reason=payload["relation"],
                before={"linked_event_seq": int(other["seq"]), "relation": payload["relation"]}, after=None,
                batch_key="", now=now,
            )
            return {"event_seq": seq, "links": store.links_for(int(event["id"]))}

    def events_for_object(self, object_type: str, object_key: str, *, limit: int) -> list[dict[str, Any]]:
        rows = self.store.connection.execute(
            "SELECT e.* FROM operational_events e JOIN event_subjects s ON s.event_id=e.id "
            "WHERE s.object_type=? AND s.object_key=? ORDER BY e.seq DESC LIMIT ?",
            (object_type, object_key, limit),
        ).fetchall()
        return [self._serialize(row, self.store) for row in rows]

    # ---- 批量归档 ----

    def archive_batch(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        criteria = {
            "statuses": list(dict.fromkeys(payload["statuses"])),
            "occurred_before": payload.get("occurred_before"),
            "satellite_code": payload.get("satellite_code"),
            "mission_code": payload.get("mission_code"),
        }
        batch_key = payload.get("batch_key") or canonical_digest({"actor": actor, **criteria, "at": now[:13]})[:32]
        with transaction(immediate=True) as connection:
            store = EventStore(connection)
            existing = connection.execute("SELECT * FROM event_archives WHERE batch_key=?", (batch_key,)).fetchone()
            if existing is not None:
                return {"batch_key": batch_key, "archived": 0, "replayed": True, "criteria": json.loads(existing["criteria_json"])}
            if payload.get("occurred_before"):
                self._normalize_occurred(payload["occurred_before"], now)
            rows = store.events_for_archive(
                statuses=criteria["statuses"], occurred_before=criteria["occurred_before"],
                satellite_code=criteria["satellite_code"], mission_code=criteria["mission_code"],
                limit=payload["limit"],
            )
            archived_seqs: list[int] = []
            for row in rows:
                store.mark_status(int(row["id"]), "archived", actor, now)
                connection.execute(
                    "UPDATE operational_events SET archive_batch=? WHERE id=?", (batch_key, row["id"])
                )
                store.add_change(
                    event_id=int(row["id"]), seq=int(row["seq"]), action="archive", actor=actor,
                    reason="批量归档", before={"status": row["status"]}, after={"status": "archived"},
                    batch_key=batch_key, now=now,
                )
                archived_seqs.append(int(row["seq"]))
            store.record_archive_batch(batch_key=batch_key, criteria=criteria, amount=len(archived_seqs), archived_by=actor, now=now)
            return {"batch_key": batch_key, "archived": len(archived_seqs), "event_seqs": archived_seqs, "replayed": False, "criteria": criteria}

    # ---- 运行摘要 ----

    def overall_summary(self) -> dict[str, Any]:
        connection = self.connection
        status_rows = connection.execute("SELECT status,COUNT(*) AS amount FROM operational_events GROUP BY status").fetchall()
        severity_rows = connection.execute("SELECT severity,COUNT(*) AS amount FROM operational_events WHERE status IN ('open','acknowledged','escalated') GROUP BY severity").fetchall()
        satellite_rows = connection.execute(
            "SELECT satellite_code,COUNT(*) AS amount FROM operational_events WHERE status IN ('open','acknowledged','escalated') GROUP BY satellite_code ORDER BY amount DESC LIMIT 10"
        ).fetchall()
        latest = connection.execute("SELECT COALESCE(MAX(seq),0) AS max_seq FROM operational_events").fetchone()
        return {
            "total_events": connection.execute("SELECT COUNT(*) FROM operational_events").fetchone()[0],
            "by_status": {r["status"]: int(r["amount"]) for r in status_rows},
            "active_by_severity": {r["severity"]: int(r["amount"]) for r in severity_rows},
            "satellites_with_active_events": [{"satellite_code": r["satellite_code"], "active": int(r["amount"])} for r in satellite_rows],
            "latest_seq": int(latest["max_seq"]),
        }

    def satellite_summary(self, satellite_code: str) -> dict[str, Any]:
        summary = self.store.satellite_summary(satellite_code)
        if summary["total_events"] == 0:
            raise NotFoundError("该卫星暂无事件")
        return summary

    def mission_summary(self, mission_code: str) -> dict[str, Any]:
        summary = self.store.mission_summary(mission_code)
        if summary["total_events"] == 0:
            raise NotFoundError("该任务暂无事件")
        return summary

    # ---- 内部辅助 ----

    def _transition(self, seq: int, actor: str, action: str, reason: str, allowed_from: set[str], target: str, mutation=None, extra_after=None) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            store = EventStore(connection)
            event = self._require_by_seq(store, seq)
            if event["status"] not in allowed_from:
                raise ConflictError(f"事件当前状态 {event['status']} 不允许{action}操作")
            if mutation is not None:
                mutation(store, event, now)
            else:
                store.mark_status(int(event["id"]), target, actor, now)
            return self._apply_transition(store, event, action, actor, reason, target, now=now, extra_after=extra_after)

    def _apply_transition(self, store: EventStore, event: sqlite3.Row, action: str, actor: str, reason: str, target: str, *, now: str, extra_after=None) -> dict[str, Any]:
        before = {"status": event["status"]}
        refreshed = store.event_by_id(int(event["id"]))
        after = {"status": target}
        if extra_after:
            after.update(extra_after)
        store.add_change(
            event_id=int(event["id"]), seq=int(event["seq"]), action=action, actor=actor,
            reason=reason, before=before, after=after, batch_key="", now=now,
        )
        return self._serialize(refreshed, store)

    @staticmethod
    def _require_by_seq(store: EventStore, seq: int) -> sqlite3.Row:
        event = store.event_by_seq(seq)
        if event is None:
            raise NotFoundError("事件不存在或已删除")
        return event

    @staticmethod
    def _normalize_occurred(value: str | None, fallback: str) -> str:
        if not value:
            return fallback
        try:
            parsed = from_storage(value)
        except (ValueError, TypeError) as exc:
            raise ValidationError("occurred_at 必须是 ISO8601 时间") from exc
        if parsed is None:
            raise ValidationError("occurred_at 必须是 ISO8601 时间")
        return to_storage(parsed)

    @staticmethod
    def _serialize(row: sqlite3.Row, store: EventStore) -> dict[str, Any]:
        result = dict(row)
        result["extra"] = json.loads(result.pop("extra_json") or "{}")
        result.pop("payload_digest", None)
        subjects = store.subjects_for([int(row["id"])])
        result["subjects"] = subjects.get(int(row["id"]), [])
        return result

    @staticmethod
    def _validate_extra_schema(schema: dict[str, dict[str, Any]]) -> None:
        allowed_types = {"integer", "number", "string", "boolean", "array"}
        for name, rule in schema.items():
            if not name or not isinstance(rule, dict) or rule.get("type") not in allowed_types:
                raise ValidationError(f"扩展字段 {name or '<empty>'} 的规则不合法")

    @staticmethod
    def _validate_extra(schema: dict[str, dict[str, Any]], supplied: dict[str, Any]) -> dict[str, Any]:
        unknown = set(supplied) - set(schema)
        if unknown:
            raise ValidationError("包含事件类型未声明的扩展字段", context={"fields": sorted(unknown)})
        normalized: dict[str, Any] = {}
        for name, rule in schema.items():
            if name not in supplied:
                if rule.get("required"):
                    raise ValidationError(f"缺少必填扩展字段：{name}")
                continue
            value = supplied[name]
            kind = rule["type"]
            valid = {
                "integer": isinstance(value, int) and not isinstance(value, bool),
                "number": isinstance(value, (int, float)) and not isinstance(value, bool),
                "string": isinstance(value, str),
                "boolean": isinstance(value, bool),
                "array": isinstance(value, list),
            }[kind]
            if not valid:
                raise ValidationError(f"扩展字段 {name} 类型不正确")
            if rule.get("choices") and value not in rule["choices"]:
                raise ValidationError(f"扩展字段 {name} 不在允许的选项中")
            if kind in {"integer", "number"}:
                if rule.get("minimum") is not None and value < rule["minimum"]:
                    raise ValidationError(f"扩展字段 {name} 小于允许的最小值")
                if rule.get("maximum") is not None and value > rule["maximum"]:
                    raise ValidationError(f"扩展字段 {name} 大于允许的最大值")
            if kind == "string" and rule.get("max_length") and len(value) > int(rule["max_length"]):
                raise ValidationError(f"扩展字段 {name} 超出长度限制")
            normalized[name] = value
        return normalized
