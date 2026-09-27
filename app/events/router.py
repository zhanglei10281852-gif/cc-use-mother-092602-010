from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse

from app.api.dependencies import current_principal
from app.core.security import Principal
from app.events.cursor import decode_cursor, query_fingerprint
from app.events.schemas import (
    AckRequest,
    ArchiveRequest,
    CloseRequest,
    EscalationRequest,
    EventCorrection,
    EventDeletion,
    EventIngest,
    EventTypeDefinitionIn,
    EventTypeDefinitionUpdate,
    LinkRequest,
    UnlinkRequest,
)
from app.events.service import EventCenterService

router = APIRouter(prefix="/api/events", tags=["地面运营事件中心"])


def service() -> EventCenterService:
    return EventCenterService()


def actor_of(principal: Principal) -> str:
    return principal.display_name or principal.username


# ---- 受控事件类型词表 ----

@router.get("/types")
def list_types(include_inactive: bool = False, principal: Principal = Depends(current_principal)) -> list[dict]:
    principal.require("events.read")
    return service().list_types(include_inactive=include_inactive)


@router.post("/types", status_code=201)
def create_type(payload: EventTypeDefinitionIn, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("events.manage")
    return service().create_type(payload.model_dump(), actor_of(principal))


@router.patch("/types/{code}")
def update_type(code: str, payload: EventTypeDefinitionUpdate, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("events.manage")
    return service().update_type(code, payload.model_dump(exclude_unset=True), actor_of(principal))


# ---- 运行摘要 ----

@router.get("/summary")
def overall_summary(principal: Principal = Depends(current_principal)) -> dict:
    principal.require("events.read")
    return service().overall_summary()


@router.get("/summary/satellites/{satellite_code}")
def satellite_summary(satellite_code: str, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("events.read")
    return service().satellite_summary(satellite_code)


@router.get("/summary/missions/{mission_code}")
def mission_summary(mission_code: str, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("events.read")
    return service().mission_summary(mission_code)


# ---- 事件写入 ----

@router.post("", status_code=201)
def ingest_event(payload: EventIngest, principal: Principal = Depends(current_principal)) -> Any:
    principal.require("events.write")
    result = service().ingest(payload.model_dump())
    if result["replayed"] or result["deduplicated"]:
        # 幂等重放或时间窗去重：返回已有事件，不新建
        return JSONResponse(status_code=200, content=result)
    return result


# ---- 时间线（游标分页） ----

@router.get("")
def timeline(
    limit: int = Query(default=50, ge=1, le=200),
    cursor: str | None = Query(default=None, description="上一页返回的不透明游标；服务重启后仍有效"),
    direction: str = Query(default="forward", pattern="^(forward|backward)$"),
    event_type: str | None = None,
    satellite_code: str | None = None,
    mission_code: str | None = None,
    status: str | None = None,
    severity: str | None = None,
    source: str | None = None,
    occurred_from: str | None = None,
    occurred_to: str | None = None,
    active: bool = Query(default=False, description="仅看未关闭事件（open/acknowledged/escalated）"),
    principal: Principal = Depends(current_principal),
) -> dict:
    principal.require("events.read")
    fingerprint_obj = {
        "event_type": event_type, "satellite_code": satellite_code, "mission_code": mission_code,
        "status": status, "severity": severity, "source": source,
        "occurred_from": occurred_from, "occurred_to": occurred_to, "active": active,
    }
    fingerprint = query_fingerprint(fingerprint_obj)
    cursor_seq = None
    if cursor:
        cursor_seq = decode_cursor(cursor, fingerprint).seq
    filters: dict[str, Any] = {
        "event_type": event_type, "satellite_code": satellite_code, "mission_code": mission_code,
        "status": status, "severity": severity, "source": source,
        "occurred_from": occurred_from, "occurred_to": occurred_to,
    }
    if active:
        filters["statuses"] = ["open", "acknowledged", "escalated"]
    return service().timeline(
        limit=limit, cursor_seq=cursor_seq, direction=direction,
        fingerprint=fingerprint, filters=filters,
    )


@router.get("/objects/{object_type}/{object_key}")
def events_for_object(object_type: str, object_key: str, limit: int = Query(50, ge=1, le=200), principal: Principal = Depends(current_principal)) -> dict:
    principal.require("events.read")
    return {"object_type": object_type, "object_key": object_key, "items": service().events_for_object(object_type, object_key, limit=limit)}


@router.get("/{seq}")
def get_event(seq: int, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("events.read")
    return service().get_event(seq)


@router.get("/{seq}/changes")
def event_changes(seq: int, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("events.read")
    return {"seq": seq, "changes": service().changes(seq)}


# ---- 生命周期流转 ----

@router.post("/{seq}/acknowledge")
def acknowledge(seq: int, payload: AckRequest, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("events.lifecycle")
    return service().acknowledge(seq, actor_of(principal), payload.note)


@router.post("/{seq}/escalate")
def escalate(seq: int, payload: EscalationRequest, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("events.lifecycle")
    return service().escalate(seq, actor_of(principal), payload.reason, payload.level)


@router.post("/{seq}/resolve")
def resolve(seq: int, payload: CloseRequest, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("events.lifecycle")
    return service().resolve(seq, actor_of(principal), payload.reason)


@router.post("/{seq}/close")
def close(seq: int, payload: CloseRequest, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("events.lifecycle")
    return service().close(seq, actor_of(principal), payload.reason)


# ---- 关联 ----

@router.post("/{seq}/links", status_code=201)
def link_event(seq: int, payload: LinkRequest, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("events.write")
    return service().link(seq, payload.model_dump(), actor_of(principal))


@router.delete("/{seq}/links")
def unlink_event(seq: int, payload: UnlinkRequest, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("events.write")
    return service().unlink(seq, payload.model_dump(), actor_of(principal))


# ---- 更正 / 删除（留痕） ----

@router.patch("/{seq}")
def correct_event(seq: int, payload: EventCorrection, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("events.manage")
    return service().correct(seq, payload.model_dump(exclude_unset=True), actor_of(principal))


@router.delete("/{seq}")
def delete_event(seq: int, payload: EventDeletion, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("events.manage")
    return service().delete(seq, payload.reason, actor_of(principal))


# ---- 批量归档 ----

@router.post("/archive")
def archive_events(payload: ArchiveRequest, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("events.manage")
    return service().archive_batch(payload.model_dump(), actor_of(principal))
