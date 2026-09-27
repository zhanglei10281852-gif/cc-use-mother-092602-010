from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

EVENT_TYPES = ("launch", "orbit_insertion", "thermal_derating", "radiation_alert", "mission_failure", "manual_intervention")
SEVERITIES = ("info", "warning", "critical")
LINK_TYPES = ("related", "cause", "resolution", "duplicate")
FIELD_VALUE_TYPES = ("string", "integer", "number", "boolean", "enum")

EventType = Literal["launch", "orbit_insertion", "thermal_derating", "radiation_alert", "mission_failure", "manual_intervention"]
Severity = Literal["info", "warning", "critical"]


class ObjectRef(BaseModel):
    object_type: Literal["satellite", "mission"]
    object_key: str = Field(min_length=1, max_length=80)
    display_name: str = Field(default="", max_length=200)
    metadata: dict[str, Any] = Field(default_factory=dict)


class EventIngest(BaseModel):
    source: str = Field(min_length=1, max_length=80)
    external_id: str = Field(min_length=1, max_length=160)
    event_type: EventType
    severity: Severity = "info"
    occurred_at: datetime
    summary: str = Field(min_length=1, max_length=500)
    satellite: str | None = Field(default=None, min_length=1, max_length=80)
    mission: str | None = Field(default=None, min_length=1, max_length=80)
    correlation_key: str = Field(default="", max_length=160)
    attributes: dict[str, Any] = Field(default_factory=dict)


class BatchIngest(BaseModel):
    items: list[EventIngest] = Field(min_length=1, max_length=200)


class FieldDefinitionCreate(BaseModel):
    field_key: str = Field(pattern=r"^[a-z][a-z0-9_.]{1,63}$")
    value_type: Literal["string", "integer", "number", "boolean", "enum"]
    allowed_values: list[str] = Field(default_factory=list, max_length=100)
    required: bool = False
    applies_to: list[EventType] = Field(default_factory=list, max_length=10)
    description: str = Field(default="", max_length=500)

    @field_validator("allowed_values")
    @classmethod
    def check_enum_values(cls, value: list[str], info) -> list[str]:
        if info.data.get("value_type") == "enum" and not value:
            raise ValueError("枚举字段必须提供 allowed_values")
        return value


class TransitionRequest(BaseModel):
    reason: str = Field(default="", max_length=1000)


class CloseRequest(BaseModel):
    resolution: str = Field(min_length=2, max_length=1000)


class CorrectionRequest(BaseModel):
    reason: str = Field(min_length=2, max_length=1000)
    summary: str | None = Field(default=None, min_length=1, max_length=500)
    severity: Severity | None = None
    event_type: EventType | None = None
    occurred_at: datetime | None = None
    correlation_key: str | None = Field(default=None, max_length=160)
    satellite: str | None = Field(default=None, max_length=80)
    mission: str | None = Field(default=None, max_length=80)
    attributes: dict[str, Any] | None = None


class DeleteRequest(BaseModel):
    reason: str = Field(min_length=2, max_length=1000)


class LinkCreate(BaseModel):
    related_event_id: int = Field(ge=1)
    link_type: Literal["related", "cause", "resolution", "duplicate"] = "related"


class ArchiveRequest(BaseModel):
    event_ids: list[int] = Field(min_length=1, max_length=500)
    reason: str = Field(default="", max_length=1000)


class CursorCreate(BaseModel):
    from_event_id: int = Field(default=0, ge=0)
