from __future__ import annotations

import json
from pathlib import Path

from app.core.time import utc_now
from app.providers.base import RunContext
from app.providers.preem.adapter import PreemAdapter

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures"


def test_preem_fetch_collects_station_details() -> None:
    class FakeResponse:
        def __init__(self, payload: dict[str, object], status_code: int = 200) -> None:
            self._payload = payload
            self.status_code = status_code

        def json(self) -> dict[str, object]:
            return self._payload

    class FakeHttpClient:
        def get(self, url: str, **kwargs: object) -> FakeResponse:
            if url == "https://www.preem.se/page-data/stationer/page-data.json":
                return FakeResponse(json.loads((FIXTURES_DIR / "preem_station_list.json").read_text()))
            if url == "https://www.preem.se/page-data/stationer/preem-nykoping/page-data.json":
                return FakeResponse(json.loads((FIXTURES_DIR / "preem_station_detail.json").read_text()))
            raise AssertionError(f"unexpected url {url}")

    adapter = PreemAdapter(http_client=FakeHttpClient())  # type: ignore[arg-type]

    fetch = adapter.fetch(RunContext(mode="full", dry_run=True))

    assert fetch.status_code == 200
    assert fetch.payload["records"] == [
        {
            "id": "preem-1",
            "name": "Preem Nykoping",
            "address": "Brunnsgatan 1",
            "city": "Nykoping",
            "postal_code": "611 32",
            "country_code": "SE",
            "latitude": 58.753,
            "longitude": 17.007,
            "phone": "0155-12 34 56",
            "services": ["adapted_for_24m_vehicles"],
            "fuel_types": ["diesel", "HVO100"],
            "opening_hours": "Mon-Fri 06:00-22:00; Sat 07:00-21:00; Sun 08:00-20:00",
            "source_url": "https://www.preem.se/stationer/preem-nykoping",
        }
    ]


def test_preem_normalize_maps_station_record() -> None:
    adapter = PreemAdapter()
    payload = {
        "records": [
            {
                "id": "preem-1",
                "name": "Preem Nykoping",
                "address": "Brunnsgatan 1",
                "city": "Nykoping",
                "postal_code": "611 32",
                "country_code": "SE",
                "latitude": 58.753,
                "longitude": 17.007,
                "phone": "0155-12 34 56",
                "services": ["adapted_for_24m_vehicles"],
                "fuel_types": ["diesel", "HVO100"],
                "opening_hours": "Mon-Fri 06:00-22:00",
                "source_url": "https://www.preem.se/stationer/nykoping",
            }
        ]
    }

    facilities, issues = adapter.normalize(payload, fetched_at=utc_now())

    assert not issues
    assert facilities[0].facility_name == "Preem Nykoping"
    assert facilities[0].facility_brand == "Preem"
    assert "diesel" in facilities[0].fuel_types
