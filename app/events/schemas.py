from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

EventTypeCode = str
Severity = Literal["info", "warning", "critical"]
EventStatus = Literal["open", "acknowledged", "escalated", "resolved", "closed", "archived"]
Relation = Literal["related", "caused_by", "duplicates", "succeeds", "blocks"]


class EventSubjectIn(BaseModel):
    object_type: str = Field(min_length=1, max_length=40, pattern=r"^[a-z][a-z0-9_-]*$")
    object_key: str = Field(min_length=1, max_length=120)
    role: str = Field(default="related", max_length=40, pattern=r"^[a-z][a-z0-9_-]*$")
    label: str = Field(default="", max_length=200)


class EventIngest(BaseModel):
    event_type: str = Field(min_length=2, max_length=64, pattern=r"^[a-z][a-z0-9_.-]*$")
    satellite_code: str = Field(min_length=1, max_length=64)
    mission_code: str = Field(default="", max_length=64)
    source: str = Field(min_length=1, max_length=64)
    severity: Severity = "info"
    title: str = Field(min_length=1, max_length=200)
    description: str = Field(default="", max_length=4000)
    occurred_at: str | None = Field(default=None, description="ISO8601；缺省为接收时刻")
    # 显式幂等键：同 source+key 的重复写入返回同一事件
    idempotency_key: str = Field(default="", max_length=160)
    # 无显式幂等键时，按时间窗（秒）+ 载荷指纹去重，0 表示不做时间窗去重
    dedup_window_seconds: int = Field(default=0, ge=0, le=86400)
    extra: dict[str, Any] = Field(default_factory=dict)
    subjects: list[EventSubjectIn] = Field(default_factory=list, max_length=50)
    root_event_seq: int | None = None

    @field_validator("extra")
    @classmethod
    def extra_must_be_object(cls, value: dict[str, Any]) -> dict[str, Any]:
        if len(value) > 100:
            raise ValueError("扩展字段数量不能超过 100 个")
        return value


class EventTypeDefinitionIn(BaseModel):
    code: str = Field(min_length=2, max_length=64, pattern=r"^[a-z][a-z0-9_.-]*$")
    name: str = Field(min_length=1, max_length=120)
    category: str = Field(default="operational", max_length=64, pattern=r"^[a-z][a-z0-9_-]*$")
    lifecycle: Literal["advisory", "alert", "actionable"] = "advisory"
    description: str = Field(default="", max_length=1000)
    extra_schema: dict[str, dict[str, Any]] = Field(default_factory=dict)


class EventTypeDefinitionUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=120)
    description: str | None = Field(default=None, max_length=1000)
    extra_schema: dict[str, dict[str, Any]] | None = None
    active: bool | None = None


class EventCorrection(BaseModel):
    reason: str = Field(min_length=2, max_length=1000)
    title: str | None = Field(default=None, min_length=1, max_length=200)
    description: str | None = Field(default=None, max_length=4000)
    severity: Severity | None = None
    event_type: str | None = Field(default=None, min_length=2, max_length=64)
    mission_code: str | None = Field(default=None, max_length=64)
    extra: dict[str, Any] | None = None
    occurred_at: str | None = None


class EventDeletion(BaseModel):
    reason: str = Field(min_length=2, max_length=1000)


class AckRequest(BaseModel):
    note: str = Field(default="", max_length=1000)


class EscalationRequest(BaseModel):
    reason: str = Field(min_length=2, max_length=1000)
    level: int = Field(default=1, ge=1, le=10)


class CloseRequest(BaseModel):
    reason: str = Field(min_length=2, max_length=1000)


class LinkRequest(BaseModel):
    linked_event_seq: int
    relation: Relation
    note: str = Field(default="", max_length=1000)


class UnlinkRequest(BaseModel):
    linked_event_seq: int
    relation: Relation


class ArchiveRequest(BaseModel):
    statuses: list[EventStatus] = Field(default_factory=lambda: ["resolved", "closed"], min_length=1, max_length=6)
    occurred_before: str | None = None
    satellite_code: str | None = Field(default=None, max_length=64)
    mission_code: str | None = Field(default=None, max_length=64)
    limit: int = Field(default=500, ge=1, le=5000)
    batch_key: str | None = Field(default=None, max_length=160)

    @field_validator("statuses")
    @classmethod
    def only_terminal_statuses(cls, value: list[str]) -> list[str]:
        forbidden = {"open", "acknowledged", "escalated", "archived"}
        chosen = [status for status in value if status in forbidden]
        if chosen:
            raise ValueError(f"以下状态不允许归档：{sorted(chosen)}")
        return value
