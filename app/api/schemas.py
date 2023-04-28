from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel


class RunCreateRequest(BaseModel):
    providers: Optional[List[str]] = None
    mode: str = "full"
    dry_run: bool = False


class GapAnalysisRequest(BaseModel):
    region: Optional[str] = None
    category: Optional[str] = None
    stale_only: bool = False


class ProviderStatusResponse(BaseModel):
    provider: str
    source_type: str
    supports_incremental: bool
    last_run_status: Optional[str] = None
    last_run_finished_at: Optional[datetime] = None
    stale: bool = False

