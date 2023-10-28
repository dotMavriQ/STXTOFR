from __future__ import annotations

import json
from pathlib import Path

from app.core.time import utc_now
from app.providers.base import RunContext
from app.providers.espresso_house.adapter import EspressoHouseAdapter


FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures"


def test_espresso_house_fetch_returns_api_payload() -> None:
    class FakeResponse:
        def __init__(self, payload: dict[str, object], status_code: int = 200) -> None:
            self._payload = payload
            self.status_code = status_code

        def json(self) -> dict[str, object]:
            return self._payload

    class FakeHttpClient:
        def get(self, url: str, **kwargs: object) -> FakeResponse:
            assert url == "https://myespressohouse.com/beproud/api/CoffeeShop/v2"
            payload = json.loads((FIXTURES_DIR / "espresso_house_fetch.json").read_text())
            return FakeResponse(payload)

    adapter = EspressoHouseAdapter(http_client=FakeHttpClient())  # type: ignore[arg-type]

    fetch = adapter.fetch(RunContext(mode="full", dry_run=True))

    assert fetch.status_code == 200
    assert fetch.payload["coffeeShops"][0]["coffeeShopId"] == "eh-1"
    assert fetch.request_url == "https://myespressohouse.com/beproud/api/CoffeeShop/v2"


def test_espresso_house_normalize_maps_store_record() -> None:
    adapter = EspressoHouseAdapter()
    payload = json.loads((FIXTURES_DIR / "espresso_house_fetch.json").read_text())

    facilities, issues = adapter.normalize(payload, fetched_at=utc_now())

    assert not issues
    assert facilities[0].facility_name == "Espresso House Centralen"
    assert "preorder" in facilities[0].services
    assert "wifi" in facilities[0].services
    assert "child_friendly" in facilities[0].services
    assert "accessible" in facilities[0].services
    assert "limited_inventory" in facilities[0].services
    assert "food" in facilities[0].services
    assert "special: 2023-12-24 08:00-14:00" in (facilities[0].opening_hours or "")


def test_espresso_house_normalize_marks_takeaway_only_sites() -> None:
    adapter = EspressoHouseAdapter()
    payload = {
        "coffeeShops": [
            {
                "coffeeShopId": "eh-2",
                "country": "Sweden",
                "coffeeShopName": "Espresso House Express",
                "address1": "Examplegatan 1",
                "city": "Gothenburg",
                "postalCode": "411 11",
                "latitude": 57.7089,
                "longitude": 11.9746,
                "takeAwayOnly": True,
                "openingHours": [],
            }
        ]
    }

    facilities, issues = adapter.normalize(payload, fetched_at=utc_now())

    assert not issues
    assert "takeaway_only" in facilities[0].services
    assert "food" not in facilities[0].services
