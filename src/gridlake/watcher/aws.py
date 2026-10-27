"""Lambda entry point: an EventBridge schedule runs the watcher once a day.

The brief for the latest complete UTC day goes to S3 (``GRIDLAKE_BRIEFS_S3_URI``) and is
announced on the brief topic (``GRIDLAKE_BRIEF_TOPIC_ARN``). The watcher only reads gold
data products; it changes nothing.
"""

from __future__ import annotations

import json
import os
from datetime import date
from typing import Any

from gridlake.config import Settings
from gridlake.watcher import BriefStore, announce, watch


def handler(event: dict[str, Any], context: Any, sns: Any = None, s3: Any = None) -> dict[str, Any]:
    settings = Settings()
    day = (event or {}).get("date")  # optional override, e.g. a manual backfill invoke
    brief = watch(
        settings,
        day=date.fromisoformat(day) if day else None,
        clearance=os.getenv("GRIDLAKE_WATCHER_CLEARANCE", "public"),
    )
    location = BriefStore(settings.briefs_dir, settings.briefs_s3_uri, s3).save(brief)
    sent = announce(settings, brief, location, sns)
    out = {
        "date": brief.date,
        "anomalies": [a.id for a in brief.detection.anomalies],
        "brief": location,
        "announced": sent,
        "model": brief.model,
    }
    print(json.dumps(out))
    return out
