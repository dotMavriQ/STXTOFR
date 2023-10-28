from __future__ import annotations

import json
from pathlib import Path

from app.core.time import utc_now
from app.providers.base import RunContext
from app.providers.ids.adapter import IDSAdapter


FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures"


def test_ids_fetch_returns_feed_payload() -> None:
    class FakeResponse:
        def __init__(self, payload: dict[str, object], status_code: int = 200) -> None:
            self._payload = payload
            self.status_code = status_code

        def json(self) -> dict[str, object]:
            return self._payload

    class FakeHttpClient:
        def get(self, url: str, **kwargs: object) -> FakeResponse:
            assert url == "https://ids.q8.com/en/get/stations.json"
            payload = json.loads((FIXTURES_DIR / "ids_stations.json").read_text())
            return FakeResponse(payload)

    adapter = IDSAdapter(http_client=FakeHttpClient())  # type: ignore[arg-type]

    fetch = adapter.fetch(RunContext(mode="full", dry_run=True))

    assert fetch.status_code == 200
    assert fetch.payload["Stations"]["Station"][0]["StationId"] == "ids-1"


def test_ids_normalize_filters_sweden_and_maps_services() -> None:
    adapter = IDSAdapter()
    payload = json.loads((FIXTURES_DIR / "ids_stations.json").read_text())

    facilities, issues = adapter.normalize(payload, fetched_at=utc_now())

    assert not issues
    assert len(facilities) == 1
    assert facilities[0].city == "Malmo"
    assert "diesel" in facilities[0].fuel_types
    assert "shop" in facilities[0].services
    assert facilities[0].notes is not None
    assert "state: open" in facilities[0].notes
    assert "lift_available: yes" in facilities[0].notes


def test_ids_normalize_maps_opening_hours_when_feed_exposes_them() -> None:
    adapter = IDSAdapter()
    payload = {
        "Stations": {
            "Station": [
                {
                    "StationId": "ids-open-1",
                    "Country": "Sweden",
                    "State": "Open",
                    "PossibilityToLift": False,
                    "Name": "IDS Open Hours",
                    "NodeURL": "/station/ids-open-1",
                    "Address_line_1": "Hamngatan 1",
                    "Address_line_2": "211 20 Malmo",
                    "Phone": "040-00 00 00",
                    "XCoordinate": 55.6049,
                    "YCoordinate": 13.0038,
                    "OpeningHours": "Mon-Sun 00:00-24:00",
                    "Services": {"Service": ["diesel", "shop"]},
                }
            ]
        }
    }

    facilities, issues = adapter.normalize(payload, fetched_at=utc_now())

    assert not issues
    assert facilities[0].opening_hours == "Mon-Sun 00:00-24:00"
    assert facilities[0].notes is not None
    assert "lift_available: no" in facilities[0].notes
