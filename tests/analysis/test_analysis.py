from datetime import datetime, timedelta

from app.analysis.service import AnalysisService
from app.normalization.models import NormalizedFacility, RawPayloadRef
from app.routing.publisher import NoopPublisher
from app.storage.repository import InMemoryRepository


def test_gap_analysis_flags_stale_and_missing_fields() -> None:
    repository = InMemoryRepository()
    facility = NormalizedFacility(
        provider_name="rasta",
        provider_record_id="arboga",
        source_type="scrape",
        source_url=None,
        raw_payload_ref=RawPayloadRef(raw_payload_id=1, provider_name="rasta"),
        facility_name="Rasta Arboga",
        facility_brand="Rasta",
        category="roadside_rest",
        city="Arboga",
        confidence_score=0.5,
        freshness_ts=datetime.utcnow() - timedelta(days=30),
        normalized_hash="abc",
    )
    repository.save_facility(facility)
    service = AnalysisService(repository=repository, publisher=NoopPublisher())
    findings = service.run_gap_analysis()
    assert findings
    assert any(finding["finding_type"] == "stale_record" for finding in findings)

