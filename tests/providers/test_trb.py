import json
from datetime import datetime
from pathlib import Path

from app.core.exceptions import ProviderFetchError
from app.providers.base import RunContext
from app.providers.trb.adapter import TRBAdapter

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures"


def test_extract_widget_uid_from_station_page_script() -> None:
    html = """
    <html>
      <body>
        <script
          id="storelocatorscript"
          data-uid="zD99kjHRWXwQtFDaUIvDfrXYnu9SezCG"
          src="//cdn.storelocatorwidgets.com/widget/widget.js"
        ></script>
      </body>
    </html>
    """
    assert TRBAdapter._extract_widget_uid(html) == "zD99kjHRWXwQtFDaUIvDfrXYnu9SezCG"


def test_decode_widget_payload_accepts_locator_jsonp() -> None:
    payload = TRBAdapter._decode_widget_payload(
        'slwapi({"locations":[{"storeid":"123","name":"TRB Test","data":{"map_lat":"56.1","map_lng":"12.7"}}]})'
    )
    assert payload["locations"][0]["storeid"] == "123"


def test_decode_widget_payload_accepts_bootstrap_jsonp() -> None:
    payload = TRBAdapter._decode_widget_payload(
        'slw({"stores":[{"storeid":"123","name":"TRB Test","data":{"map_lat":"56.1","map_lng":"12.7"}}]})'
    )
    assert payload["stores"][0]["storeid"] == "123"


def test_extract_records_flattens_widget_stores() -> None:
    payload = {
        "stores": [
            {
                "storeid": "123",
                "name": "TRB Helsingborg",
                "description": "<p>Truck stop</p>",
                "data": {
                    "map_lat": "56.05",
                    "map_lng": "12.70",
                    "address": "Terminalgatan 1",
                    "city": "Helsingborg",
                    "zip": "252 78",
                    "phone": "042-123 45",
                    "services": "lastbilsparkering,toalett,dusch",
                    "fuels": "diesel,hvo100,adblue",
                },
            }
        ]
    }
    records = TRBAdapter._extract_records(payload, uid="uid-1")
    assert records == [
        {
            "id": "123",
            "name": "TRB Helsingborg",
            "latitude": 56.05,
            "longitude": 12.7,
            "address": "Terminalgatan 1",
            "city": "Helsingborg",
            "postal_code": "252 78",
            "region": None,
            "phone": "042-123 45",
            "opening_hours": None,
            "services": ["lastbilsparkering", "toalett", "dusch"],
            "fuels": ["diesel", "hvo100", "adblue"],
            "description": "Truck stop",
            "source_url": "https://trb.se/hitta-tankstation/?uid=uid-1",
        }
    ]


def test_extract_records_still_accepts_locator_locations() -> None:
    payload = {
        "locations": [
            {
                "storeid": "123",
                "name": "TRB Helsingborg",
                "description": "<p>Truck stop</p>",
                "data": {
                    "map_lat": "56.05",
                    "map_lng": "12.70",
                    "address": "Terminalgatan 1",
                    "city": "Helsingborg",
                    "zip": "252 78",
                    "phone": "042-123 45",
                    "services": "lastbilsparkering,toalett,dusch",
                    "fuels": "diesel,hvo100,adblue",
                },
            }
        ]
    }
    records = TRBAdapter._extract_records(payload, uid="uid-1")
    assert records == [
        {
            "id": "123",
            "name": "TRB Helsingborg",
            "latitude": 56.05,
            "longitude": 12.7,
            "address": "Terminalgatan 1",
            "city": "Helsingborg",
            "postal_code": "252 78",
            "region": None,
            "phone": "042-123 45",
            "opening_hours": None,
            "services": ["lastbilsparkering", "toalett", "dusch"],
            "fuels": ["diesel", "hvo100", "adblue"],
            "description": "Truck stop",
            "source_url": "https://trb.se/hitta-tankstation/?uid=uid-1",
        }
    ]


def test_normalize_maps_trb_fixture_into_facility() -> None:
    payload = json.loads((FIXTURES_DIR / "trb_locations.json").read_text(encoding="utf-8"))
    facilities, issues = TRBAdapter().normalize(payload, fetched_at=datetime(2023, 10, 20, 12, 0, 0))
    assert len(facilities) == 1
    facility = facilities[0]
    assert facility.provider_name == "trb"
    assert facility.facility_name == "TRB Helsingborg"
    assert facility.latitude == 56.05
    assert facility.longitude == 12.7
    assert facility.city == "Helsingborg"
    assert facility.country_code == "se"
    assert not issues


def test_fetch_falls_back_to_browser_bootstrap_payload() -> None:
    class FakeResponse:
        def __init__(self, text: str, status_code: int = 200) -> None:
            self.text = text
            self.status_code = status_code

    class FakeHttpClient:
        def get(self, url: str, **kwargs: object) -> FakeResponse:
            if url == TRBAdapter.STATION_PAGE_URL:
                return FakeResponse(
                    """
                    <html>
                      <body>
                        <script
                          id="storelocatorscript"
                          data-uid="uid-123"
                          src="//cdn.storelocatorwidgets.com/widget/widget.js"
                        ></script>
                      </body>
                    </html>
                    """
                )
            if url == TRBAdapter.WIDGET_JSON_URL_TEMPLATE.format(uid="uid-123"):
                raise ProviderFetchError("network blocked")
            raise AssertionError(f"unexpected url {url}")

    adapter = TRBAdapter(http_client=FakeHttpClient())  # type: ignore[arg-type]
    adapter._fetch_widget_payload_with_browser = lambda uid: (
        'slw({"stores":[{"storeid":"123","name":"TRB Test","data":{"map_lat":"56.1","map_lng":"12.7"}}]})'
    )

    fetch = adapter.fetch(RunContext(mode="full", dry_run=True))

    assert fetch.status_code == 200
    assert fetch.payload["uid"] == "uid-123"
    assert fetch.payload["records"][0]["id"] == "123"
    assert fetch.payload["records"][0]["latitude"] == 56.1
    assert fetch.request_headers["x-trb-fetch-mode"] == "browser-fallback"
