import json
from datetime import datetime
from pathlib import Path

from app.providers.base import RunContext
from app.providers.trafikverket.adapter import TrafikverketParkingAdapter

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures"


def test_extract_records_reads_parking_response() -> None:
    payload = json.loads((FIXTURES_DIR / "trafikverket_parking.json").read_text(encoding="utf-8"))
    records = TrafikverketParkingAdapter._extract_records(payload)
    assert len(records) == 1
    record = records[0]
    assert record["id"] == "SE_STA_TRISSID_1_4048561"
    assert record["name"] == "Träffpunkt Gotland"
    assert record["latitude"] == 57.622858
    assert record["longitude"] == 18.274728
    assert "refusebin" in record["equipment"]
    assert "truckparking" in record["usage_scenarios"]


def test_trafikverket_fetch_posts_query_and_returns_records() -> None:
    class FakeResponse:
        def __init__(self, payload: dict[str, object], status_code: int = 200) -> None:
            self._payload = payload
            self.status_code = status_code

        def json(self) -> dict[str, object]:
            return self._payload

    class FakeHttpClient:
        def __init__(self) -> None:
            self.last_data: bytes | None = None
            self.last_headers: dict[str, str] | None = None

        def post(self, url: str, **kwargs: object) -> FakeResponse:
            assert url == "https://api.trafikinfo.trafikverket.se/v2/data.json"
            self.last_data = kwargs.get("data")  # type: ignore[assignment]
            self.last_headers = kwargs.get("headers")  # type: ignore[assignment]
            payload = json.loads((FIXTURES_DIR / "trafikverket_parking.json").read_text(encoding="utf-8"))
            return FakeResponse(payload)

    http_client = FakeHttpClient()
    adapter = TrafikverketParkingAdapter(http_client=http_client)  # type: ignore[arg-type]
    adapter._extract_public_api_key = lambda: "public-key"

    fetch = adapter.fetch(RunContext(mode="full", dry_run=True))

    assert fetch.status_code == 200
    assert fetch.payload["records"][0]["id"] == "SE_STA_TRISSID_1_4048561"
    assert fetch.request_headers == {"content-type": "text/xml"}
    assert http_client.last_data is not None
    assert b'authenticationkey="public-key"' in http_client.last_data
    assert http_client.last_headers is not None
    assert http_client.last_headers["content-type"] == "text/xml"


def test_normalize_maps_rest_area_into_facility() -> None:
    payload = {"records": TrafikverketParkingAdapter._extract_records(
        json.loads((FIXTURES_DIR / "trafikverket_parking.json").read_text(encoding="utf-8"))
    )}
    facilities, issues = TrafikverketParkingAdapter().normalize(
        payload,
        fetched_at=datetime(2023, 10, 20, 12, 0, 0),
    )
    assert len(facilities) == 1
    facility = facilities[0]
    assert facility.provider_name == "trafikverket"
    assert facility.category == "parking"
    assert facility.subcategories == ["rest_area"]
    assert facility.latitude == 57.622858
    assert facility.longitude == 18.274728
    assert facility.phone == "0771-921 921"
    assert facility.country_code == "se"
    assert facility.heavy_vehicle_relevance is True
    assert "refusebin" in facility.services
    assert "lorry:2" in facility.parking_features
    assert not issues
