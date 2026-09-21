"""告警去重指纹：稳定、可解释（同一输入永远得到同一指纹）。"""

from __future__ import annotations

import hashlib
import json

from .models import AlertEvent

_FINGERPRINT_VERSION = "fp1"


def alert_fingerprint(event: AlertEvent) -> str:
    """由 source/resource/severity/labels/title 派生稳定指纹。

    刻意排除 fired_at 与 alert_id：同一问题在短时间内被重复上报
    （每次事件 id 不同、时间戳不同）必须得到相同指纹，从而被去重。
    labels 按 key 排序后参与哈希，保证字典插入顺序不影响结果。
    """
    canonical = json.dumps(
        {
            "v": _FINGERPRINT_VERSION,
            "source": event.source,
            "resource_id": event.resource_id,
            "severity": event.severity,
            "title": event.title,
            "labels": dict(sorted(event.labels.items())),
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
    return f"{_FINGERPRINT_VERSION}:{digest}"
