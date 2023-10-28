from __future__ import annotations

from app.core.time import utc_now
from app.normalization.models import NormalizedFacility, RawPayloadRef
from app.services.curation import CurationService
from app.services.facility_view import FacilityViewService
from app.storage.repository import InMemoryRepository


class FakeBaserowClient:
    def __init__(self, rows: list[dict[str, object]] | None = None) -> None:
        self.rows = rows or []
        self.created_payloads: list[dict[str, object]] = []
        self.updated_payloads: list[tuple[int, dict[str, object]]] = []

    def list_rows(self) -> list[dict[str, object]]:
        return list(self.rows)

    def create_row(self, fields: dict[str, object]) -> dict[str, object]:
        row = {"id": len(self.rows) + 1, **fields}
        self.rows.append(row)
        self.created_payloads.append(fields)
        return row

    def update_row(self, row_id: int, fields: dict[str, object]) -> dict[str, object]:
        self.updated_payloads.append((row_id, fields))
        for row in self.rows:
            if row["id"] == row_id:
                row.update(fields)
                return row
        raise AssertionError("row not found")

    def ensure_review_schema(self) -> dict[str, object]:
        return {"status": "completed", "created_count": 0}


def _build_service(client: FakeBaserowClient) -> tuple[InMemoryRepository, CurationService]:
    repository = InMemoryRepository()
    repository.save_facility(
        NormalizedFacility(
            provider_name="circlek",
            provider_record_id="1002",
            source_type="api",
            source_url="https://www.circlek.se/station-search",
            raw_payload_ref=RawPayloadRef(raw_payload_id=1, provider_name="circlek"),
            facility_name="Circle K Test",
            facility_brand="Circle K",
            category="fuel_station",
            latitude=59.3324,
            longitude=18.0586,
            formatted_address="Testgatan 1, Stockholm",
            street="Testgatan 1",
            city="Stockholm",
            postal_code="111 21",
            country_code="se",
            phone="+460000",
            opening_hours="24/7",
            services=["toalett"],
            freshness_ts=utc_now(),
            normalized_hash="hash-1002",
        )
    )
    service = CurationService(
        repository=repository,
        facility_view_service=FacilityViewService(repository),
        baserow_client=client,
    )
    return repository, service


def test_push_to_baserow_creates_review_rows_without_duplication() -> None:
    client = FakeBaserowClient()
    repository, service = _build_service(client)

    result = service.push_to_baserow()

    assert result["status"] == "completed"
    assert result["created_count"] == 1
    assert client.rows[0]["stxtofr_key"] == "source:1"
    assert client.rows[0]["longitude"] == 18.0586
    curation = repository.get_facility_curation(1)
    assert curation is not None
    assert curation["baserow_row_id"] == 1


def test_push_to_baserow_rounds_coordinate_float_tails() -> None:
    client = FakeBaserowClient()
    repository, service = _build_service(client)
    repository.facilities[0]["longitude"] = 12.937241599999993

    service.push_to_baserow()

    assert client.rows[0]["longitude"] == 12.9372416


def test_pull_from_baserow_stores_overrides_and_manual_rows() -> None:
    client = FakeBaserowClient(
        rows=[
            {
                "id": 10,
                "row_origin": "source",
                "source_facility_id": 1,
                "facility_name": "Circle K Curated",
                "category": "fuel_station",
                "formatted_address": "Testgatan 1, Stockholm",
                "street": "Testgatan 1",
                "city": "Stockholm",
                "region": None,
                "postal_code": "111 21",
                "latitude": 59.5,
                "longitude": 18.1,
                "phone": "+460000",
                "opening_hours": "24/7",
                "services": ["toalett", "wifi"],
                "notes": "human checked",
                "verified_status": "verified",
            },
            {
                "id": 11,
                "row_origin": "manual",
                "facility_name": "Manual Truck Stop",
                "category": "fuel_station",
                "formatted_address": "Hamngatan 1, Malmo",
                "city": "Malmo",
                "latitude": 55.6,
                "longitude": 13.0,
                "services": ["parking"],
                "verified_status": "unverified",
            },
        ]
    )
    repository, service = _build_service(client)

    result = service.pull_from_baserow()

    assert result["status"] == "completed"
    assert result["pulled_count"] == 2
    assert repository.get_facility_curation(1)["facility_name"] == "Circle K Curated"
    assert repository.get_facility_curation(1)["latitude"] == 59.5
    assert repository.list_manual_facilities()[0]["facility_name"] == "Manual Truck Stop"


def test_pull_from_baserow_ignores_coordinate_precision_only_changes() -> None:
    client = FakeBaserowClient(
        rows=[
            {
                "id": 10,
                "row_origin": "source",
                "source_facility_id": 1,
                "facility_name": "Circle K Test",
                "category": "fuel_station",
                "formatted_address": "Testgatan 1, Stockholm",
                "street": "Testgatan 1",
                "city": "Stockholm",
                "postal_code": "111 21",
                "latitude": 59.3324,
                "longitude": 18.0586000,
                "phone": "+460000",
                "opening_hours": "24/7",
                "services": ["toalett"],
                "verified_status": "unverified",
            },
        ]
    )
    repository, service = _build_service(client)
    repository.facilities[0]["longitude"] = 18.058600000000001

    service.pull_from_baserow()

    assert repository.get_facility_curation(1)["longitude"] is None


def test_bootstrap_baserow_schema_delegates_to_client() -> None:
    client = FakeBaserowClient()
    _, service = _build_service(client)

    assert service.bootstrap_baserow_schema() == {"status": "completed", "created_count": 0}
