from __future__ import annotations

import base64
import hashlib
import json
from typing import Any

from app.core.errors import ValidationError

"""游标令牌：把分页位置与查询条件指纹编码为不透明字符串。

游标中带有查询条件指纹，客户端在翻页途中改变过滤条件会被拒绝，
避免把两个不同查询的分页位置拼接在一起造成重复或漏项。
"""

CURSOR_VERSION = 1


def filter_fingerprint(filters: dict[str, Any]) -> str:
    compact = json.dumps(filters, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(compact.encode()).hexdigest()[:16]


def encode_cursor(*, sort: str, fingerprint: str, last_id: int, last_occurred_at: str | None = None) -> str:
    payload = {
        "v": CURSOR_VERSION,
        "sort": sort,
        "f": fingerprint,
        "id": last_id,
        "occ": last_occurred_at,
    }
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def decode_cursor(token: str, *, expected_sort: str, expected_fingerprint: str) -> dict[str, Any]:
    try:
        padded = token + "=" * (-len(token) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded.encode()).decode())
    except (ValueError, UnicodeDecodeError) as exc:
        raise ValidationError("游标格式不合法") from exc
    if not isinstance(payload, dict) or payload.get("v") != CURSOR_VERSION:
        raise ValidationError("游标版本不受支持")
    if payload.get("sort") != expected_sort:
        raise ValidationError("游标排序方式与当前查询不一致")
    if payload.get("f") != expected_fingerprint:
        raise ValidationError("查询条件与游标不匹配，请从第一页重新查询")
    last_id = payload.get("id")
    if not isinstance(last_id, int) or last_id < 0:
        raise ValidationError("游标位置不合法")
    return {"last_id": last_id, "last_occurred_at": payload.get("occ")}
