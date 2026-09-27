from __future__ import annotations

from app.database import get_connection

SCHEMA = """
CREATE TABLE IF NOT EXISTS event_field_definitions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    field_key TEXT NOT NULL UNIQUE,
    value_type TEXT NOT NULL CHECK(value_type IN ('string','integer','number','boolean','enum')),
    allowed_values_json TEXT NOT NULL DEFAULT '[]',
    required INTEGER NOT NULL DEFAULT 0 CHECK(required IN (0,1)),
    applies_to_json TEXT NOT NULL DEFAULT '[]',
    description TEXT NOT NULL DEFAULT '',
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS event_objects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    object_type TEXT NOT NULL CHECK(object_type IN ('satellite','mission')),
    object_key TEXT NOT NULL,
    display_name TEXT NOT NULL DEFAULT '',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(object_type, object_key)
);

CREATE TABLE IF NOT EXISTS ops_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    external_id TEXT NOT NULL,
    event_type TEXT NOT NULL CHECK(event_type IN ('launch','orbit_insertion','thermal_derating','radiation_alert','mission_failure','manual_intervention')),
    severity TEXT NOT NULL DEFAULT 'info' CHECK(severity IN ('info','warning','critical')),
    status TEXT NOT NULL DEFAULT 'open' CHECK(status IN ('open','acknowledged','escalated','closed','archived')),
    satellite_id INTEGER REFERENCES event_objects(id),
    mission_id INTEGER REFERENCES event_objects(id),
    occurred_at TEXT NOT NULL,
    summary TEXT NOT NULL,
    attributes_json TEXT NOT NULL DEFAULT '{}',
    correlation_key TEXT NOT NULL DEFAULT '',
    payload_digest TEXT NOT NULL,
    archived_at TEXT,
    deleted_at TEXT,
    version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(source, external_id)
);
CREATE INDEX IF NOT EXISTS idx_ops_events_timeline ON ops_events(occurred_at, id);
CREATE INDEX IF NOT EXISTS idx_ops_events_satellite ON ops_events(satellite_id, status);
CREATE INDEX IF NOT EXISTS idx_ops_events_mission ON ops_events(mission_id, status);
CREATE INDEX IF NOT EXISTS idx_ops_events_correlation ON ops_events(correlation_key) WHERE correlation_key <> '';

CREATE TABLE IF NOT EXISTS ops_event_links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id INTEGER NOT NULL REFERENCES ops_events(id) ON DELETE RESTRICT,
    related_event_id INTEGER NOT NULL REFERENCES ops_events(id) ON DELETE RESTRICT,
    link_type TEXT NOT NULL DEFAULT 'related' CHECK(link_type IN ('related','cause','resolution','duplicate')),
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(event_id, related_event_id, link_type)
);

CREATE TABLE IF NOT EXISTS ops_event_journal (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id INTEGER NOT NULL REFERENCES ops_events(id) ON DELETE RESTRICT,
    action TEXT NOT NULL CHECK(action IN ('acknowledge','escalate','close','archive','correct','delete','restore')),
    actor TEXT NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    before_json TEXT NOT NULL,
    after_json TEXT NOT NULL,
    batch_key TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ops_event_journal_event ON ops_event_journal(event_id, id);

CREATE TABLE IF NOT EXISTS ops_event_cursors (
    name TEXT PRIMARY KEY,
    last_event_id INTEGER NOT NULL DEFAULT 0,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""


def ensure_schema() -> None:
    connection = get_connection()
    connection.executescript(SCHEMA)
