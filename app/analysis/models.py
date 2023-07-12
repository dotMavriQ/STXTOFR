from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass(frozen=True)
class GapFinding:
    finding_type: str
    provider_name: str | None
    category: str
    region: str
    severity: str
    message: str
    facility_id: int | None = None
    created_at: datetime = field(default_factory=datetime.utcnow)

