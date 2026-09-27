from __future__ import annotations

from fastapi import APIRouter, Depends, Header, Query, Response

from app.api.dependencies import current_principal
from app.core.security import Principal
from app.database import get_connection
from app.events.schemas import (
    ArchiveRequest,
    BatchIngest,
    CloseRequest,
    CorrectionRequest,
    CursorCreate,
    DeleteRequest,
    EventIngest,
    FieldDefinitionCreate,
    LinkCreate,
    ObjectRef,
    TransitionRequest,
)
from app.events.service import EventCenterService
from app.services.idempotency import IdempotencyService

router = APIRouter(prefix="/api/events", tags=["地面运营事件中心"])

INGEST_SCOPE = "events.ingest"
BATCH_SCOPE = "events.ingest_batch"


def service() -> EventCenterService:
    return EventCenterService(get_connection())


def _ingest_response(payload: EventIngest) -> tuple[dict, int]:
    event, created = service().ingest(payload.model_dump())
    return {"event": event, "deduplicated": not created, "idempotent_replay": False}, (201 if created else 200)


# ---- 事件接入 ----


@router.post("", status_code=201)
def ingest_event(
    payload: EventIngest,
    response: Response,
    principal: Principal = Depends(current_principal),
    idempotency_key: str | None = Header(default=None, max_length=160),
) -> dict:
    principal.require("events.write")
    if idempotency_key:
        stored = IdempotencyService(get_connection()).execute(
            INGEST_SCOPE, idempotency_key, payload.model_dump(mode="json"), lambda: _ingest_response(payload)
        )
        response.status_code = stored.status_code
        body = dict(stored.body)
        body["idempotent_replay"] = stored.replayed
        return body
    body, status_code = _ingest_response(payload)
    response.status_code = status_code
    return body


@router.post("/batch")
def ingest_batch(
    payload: BatchIngest,
    principal: Principal = Depends(current_principal),
    idempotency_key: str | None = Header(default=None, max_length=160),
) -> dict:
    principal.require("events.write")
    if idempotency_key:
        stored = IdempotencyService(get_connection()).execute(
            BATCH_SCOPE,
            idempotency_key,
            payload.model_dump(mode="json"),
            lambda: (service().ingest_batch([item.model_dump() for item in payload.items]), 200),
        )
        body = dict(stored.body)
        body["idempotent_replay"] = stored.replayed
        return body
    result = service().ingest_batch([item.model_dump() for item in payload.items])
    result["idempotent_replay"] = False
    return result


# ---- 时间线查询与详情 ----


@router.get("")
def list_events(
    limit: int = Query(default=50, ge=1, le=200),
    cursor: str | None = None,
    sort: str = Query(default="occurred", pattern="^(occurred|ingested)$"),
    satellite: str | None = None,
    mission: str | None = None,
    event_type: str | None = None,
    severity: str | None = None,
    status: list[str] | None = Query(default=None),
    source: str | None = None,
    correlation_key: str | None = None,
    include_deleted: bool = False,
    principal: Principal = Depends(current_principal),
) -> dict:
    principal.require("events.read")
    return service().timeline(
        limit=limit, cursor_token=cursor, sort=sort, satellite=satellite, mission=mission,
        event_type=event_type, severity=severity, statuses=status, source=source,
        correlation_key=correlation_key, include_deleted=include_deleted,
    )


@router.get("/overview")
def overview(principal: Principal = Depends(current_principal)) -> dict:
    principal.require("events.read")
    return service().overview()


@router.get("/summary/{dimension}")
def summary(dimension: str, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("events.read")
    return service().summary(dimension)


@router.get("/{event_id}")
def get_event(event_id: int, include_deleted: bool = False, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("events.read")
    return service().get_event(event_id, include_deleted=include_deleted)


@router.get("/{event_id}/journal")
def get_journal(event_id: int, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("events.read")
    return {"items": service().journal(event_id)}


# ---- 处置流转 ----


@router.post("/{event_id}/acknowledge")
def acknowledge(event_id: int, payload: TransitionRequest, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("events.operate")
    return service().acknowledge(event_id, principal.username, payload.reason)


@router.post("/{event_id}/escalate")
def escalate(event_id: int, payload: TransitionRequest, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("events.operate")
    return service().escalate(event_id, principal.username, payload.reason)


@router.post("/{event_id}/close")
def close(event_id: int, payload: CloseRequest, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("events.operate")
    return service().close(event_id, principal.username, payload.resolution)


@router.post("/{event_id}/archive")
def archive(event_id: int, payload: TransitionRequest, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("events.operate")
    return service().archive(event_id, principal.username, payload.reason)


@router.post("/archive-batch")
def archive_batch(payload: ArchiveRequest, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("events.operate")
    return service().batch_archive(payload.event_ids, principal.username, payload.reason)


@router.post("/{event_id}/corrections")
def correct(event_id: int, payload: CorrectionRequest, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("events.operate")
    return service().correct(event_id, principal.username, payload.model_dump(exclude_unset=True))


@router.delete("/{event_id}")
def delete(event_id: int, payload: DeleteRequest, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("events.operate")
    return service().soft_delete(event_id, principal.username, payload.reason)


@router.post("/{event_id}/restore")
def restore(event_id: int, payload: TransitionRequest, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("events.operate")
    return service().restore(event_id, principal.username, payload.reason)


@router.post("/{event_id}/links", status_code=201)
def link(event_id: int, payload: LinkCreate, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("events.write")
    return service().link_events(event_id, payload.model_dump(), principal.username)


# ---- 扩展字段与关联对象 ----


@router.get("/fields/registry")
def list_fields(principal: Principal = Depends(current_principal)) -> dict:
    principal.require("events.read")
    return {"items": service().list_field_definitions()}


@router.post("/fields/registry", status_code=201)
def create_field(payload: FieldDefinitionCreate, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("events.write")
    return service().create_field_definition(payload.model_dump(), principal.username)


@router.get("/objects/catalog")
def list_objects(object_type: str | None = None, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("events.read")
    return {"items": service().list_objects(object_type)}


@router.put("/objects/catalog")
def upsert_object(payload: ObjectRef, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("events.write")
    return service().upsert_object(payload.model_dump())


# ---- 历史回放与命名游标 ----


@router.get("/replay/from/{after_event_id}")
def replay_from(after_event_id: int, limit: int = Query(default=100, ge=1, le=500), principal: Principal = Depends(current_principal)) -> dict:
    principal.require("events.read")
    return service().replay_from(after_event_id, limit)


@router.get("/cursors/named")
def list_cursors(principal: Principal = Depends(current_principal)) -> dict:
    principal.require("events.read")
    return {"items": service().list_playback_cursors()}


@router.put("/cursors/named/{name}")
def create_cursor(name: str, payload: CursorCreate, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("events.read")
    return service().create_playback_cursor(name, payload.from_event_id, principal.username)


@router.get("/cursors/named/{name}")
def get_cursor(name: str, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("events.read")
    return service().get_playback_cursor(name)


@router.post("/cursors/named/{name}/next")
def replay_next(name: str, limit: int = Query(default=100, ge=1, le=500), principal: Principal = Depends(current_principal)) -> dict:
    principal.require("events.read")
    return service().replay(name, limit)


@router.delete("/cursors/named/{name}")
def delete_cursor(name: str, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("events.read")
    return service().delete_playback_cursor(name)
