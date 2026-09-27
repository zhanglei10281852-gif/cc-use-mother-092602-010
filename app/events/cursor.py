from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import dataclass

from app.core.errors import ValidationError

# 游标是无状态的不透明令牌，只承载位置（seq）与查询指纹，
# 不依赖进程内存，因此服务重启后旧游标仍可继续翻页。


def encode_cursor(*, seq: int, fingerprint: str) -> str:
    body = json.dumps({"v": 1, "seq": seq, "f": fingerprint}, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(body).decode().rstrip("=")


@dataclass(frozen=True, slots=True)
class Cursor:
    seq: int
    fingerprint: str


def decode_cursor(token: str, fingerprint: str) -> Cursor:
    try:
        padding = "=" * (-len(token) % 4)
        body = json.loads(base64.urlsafe_b64decode(token + padding).decode())
        seq = int(body["seq"])
        token_fingerprint = str(body["f"])
    except (ValueError, TypeError, KeyError) as exc:
        raise ValidationError("分页游标不合法") from exc
    if seq < 0:
        raise ValidationError("分页游标不合法")
    if token_fingerprint != fingerprint:
        raise ValidationError("分页游标的查询条件已变化，请从第一页重新开始")
    return Cursor(seq=seq, fingerprint=fingerprint)


def query_fingerprint(params: dict) -> str:
    compact = json.dumps(params, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(compact.encode()).hexdigest()[:16]
