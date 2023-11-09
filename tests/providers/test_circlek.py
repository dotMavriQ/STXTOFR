import json
from pathlib import Path

from app.core.time import utc_now
from app.providers.base import RunContext
from app.providers.circlek.adapter import CircleKAdapter

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures"


def test_circlek_normalize_maps_station_name() -> None:
    adapter = CircleKAdapter()
    payload = json.loads((FIXTURES_DIR / "circlek_station_search.json").read_text())
    facilities, issues = adapter.normalize(payload, fetched_at=utc_now())
    assert not issues
    assert facilities[0].facility_name == "Circle K Arlandastad"
    assert "diesel" in facilities[0].fuel_types


def test_circlek_fetch_extracts_records_from_station_page() -> None:
    class FakeResponse:
        def __init__(self, text: str, status_code: int = 200) -> None:
            self.text = text
            self.status_code = status_code

    class FakeHttpClient:
        def get(self, url: str, **kwargs: object) -> FakeResponse:
            assert url == "https://www.circlek.se/station-search"
            return FakeResponse((FIXTURES_DIR / "circlek_station_search_page.html").read_text())

    adapter = CircleKAdapter(http_client=FakeHttpClient())  # type: ignore[arg-type]

    fetch = adapter.fetch(RunContext(mode="full", dry_run=True))

    assert fetch.status_code == 200
    assert fetch.payload["records"][0]["site_id"] == "1001"
    assert fetch.payload["records"][0]["city"] == "Arlandastad"
    assert fetch.payload["records"][0]["fuels"] == ["diesel", "hvo100", "petrol_95"]


def test_circlek_normalize_corrects_swapped_coordinates() -> None:
    adapter = CircleKAdapter()
    payload = {
        "records": [
            {
                "site_id": "1002",
                "name": "Circle K Test",
                "street": "Testgatan 1",
                "city": "Stockholm",
                "postal_code": "111 21",
                "region": "Stockholms Län",
                "country_code": "SE",
                "latitude": 18.0586,
                "longitude": 59.3324,
                "phones": [],
                "fuels": ["diesel"],
                "services": [],
                "opening_hours": "Mon-Fri 06:00-22:00",
                "source_url": "https://www.circlek.se/station-search",
            }
        ]
    }
    facilities, issues = adapter.normalize(payload, fetched_at=utc_now())
    assert facilities[0].latitude == 59.3324
    assert facilities[0].longitude == 18.0586
    assert issues
    assert facilities[0].notes is not None


def test_circlek_normalize_skips_placeholder_station_name() -> None:
    adapter = CircleKAdapter()
    payload = {
        "records": [
            {
                "site_id": "missing-name-1",
                "name": None,
                "street": "Testgatan 1",
                "city": "Stockholm",
                "postal_code": "111 21",
                "region": "Stockholms Län",
                "country_code": "SE",
                "latitude": 59.3324,
                "longitude": 18.0586,
                "phones": [],
                "fuels": ["diesel"],
                "services": [],
                "opening_hours": "Mon-Fri 06:00-22:00",
                "source_url": "https://www.circlek.se/station-search",
            }
        ]
    }

    facilities, issues = adapter.normalize(payload, fetched_at=utc_now())

    assert facilities == []
    assert any(issue.record_id == "missing-name-1" for issue in issues)
    assert any("missing a valid display name" in issue.message for issue in issues)


def test_circlek_extract_station_payload_from_drupal_settings() -> None:
    adapter = CircleKAdapter()
    html = (FIXTURES_DIR / "circlek_station_search_page.html").read_text()
    payload = adapter._extract_station_payload(html)
    assert payload["records"][0]["site_id"] == "1001"
    assert payload["records"][0]["city"] == "Arlandastad"


def test_circlek_extract_station_payload_supports_live_drupal_shapes() -> None:
    adapter = CircleKAdapter()
    html = """
    <html>
      <body>
        <script data-drupal-selector="drupal-settings-json" type="application/json">
          {
            "ck_sim_search": {
              "station_results": {
                "2001": {
                  "/sites/2001": {"name": "CIRCLE K TEST"},
                  "/sites/2001/addresses": {
                    "PHYSICAL": {
                      "street": "Testgatan 2",
                      "postalCode": "123 45",
                      "city": "Teststad",
                      "county": "Stockholms län",
                      "country": "SE"
                    }
                  },
                  "/sites/2001/business-info": {
                    "stationFormat": "Full Service Station",
                    "clusterName": "Highway",
                    "companyName": "Circle K Sverige AB",
                    "chainConvenience": true
                  },
                  "/sites/2001/contact-details": {
                    "phones": {"WOR": "+4620320325", "OFF": "+468000000"}
                  },
                  "/sites/2001/fuels": [
                    {"name": "EU_EV_CHARGER", "displayName": "Snabbladdning"},
                    {"name": "EU_MILES_DIESEL", "displayName": "miles diesel"}
                  ],
                  "/sites/2001/services": [
                    {"name": "EU_TRUCK_PARKING", "displayName": "Lastbilsparkering"},
                    {"name": "EU_TOILETS_BOTH", "displayName": "Toalett"}
                  ],
                  "/sites/2001/location": {"lat": "59.1", "lng": "18.2"},
                  "/sites/2001/opening-info": {
                    "alwaysOpen": false,
                    "openingTimesStore": {
                      "weekdays": {"open": "06:00", "close": "22:00"},
                      "saturday": {"open": "08:00", "close": "20:00"},
                      "sunday": {"open": "08:00", "close": "20:00"}
                    },
                    "openingTimesFuel": {
                      "weekdays": {"open": "00:00", "close": "24:00"},
                      "saturday": {"open": "00:00", "close": "24:00"},
                      "sunday": {"open": "00:00", "close": "24:00"}
                    }
                  }
                }
              }
            }
          }
        </script>
      </body>
    </html>
    """
    payload = adapter._extract_station_payload(html)
    record = payload["records"][0]
    assert record["street"] == "Testgatan 2"
    assert record["postal_code"] == "123 45"
    assert record["phones"] == ["+4620320325", "+468000000"]
    assert record["fuels"] == ["fast_charging", "diesel"]
    assert record["services"] == ["truck_parking", "toilet"]
    assert record["opening_hours"] == "Store Mon-Fri 06:00-22:00; Sat 08:00-20:00; Sun 08:00-20:00 | Fuel Mon-Fri 24h; Sat 24h; Sun 24h"
    assert "station_format: Full Service Station" in record["notes"]


def test_circlek_normalize_uses_canonical_features_and_business_notes() -> None:
    adapter = CircleKAdapter()
    payload = {
        "records": [
            {
                "site_id": "rich-1",
                "name": "Circle K Rich",
                "street": "Testgatan 2",
                "city": "Teststad",
                "postal_code": "123 45",
                "region": "Stockholms län",
                "country_code": "SE",
                "latitude": 59.1,
                "longitude": 18.2,
                "phones": ["+4620320325"],
                "fuels": ["fast_charging", "diesel"],
                "services": ["truck_parking", "toilet"],
                "opening_hours": "Store Mon-Fri 06:00-22:00",
                "notes": "station_format: Full Service Station; cluster: Highway",
                "source_url": "https://www.circlek.se/station-search",
            }
        ]
    }

    facilities, issues = adapter.normalize(payload, fetched_at=utc_now())

    assert not issues
    assert facilities[0].fuel_types == ["fast_charging", "diesel"]
    assert facilities[0].services == ["truck_parking", "toilet"]
    assert facilities[0].electric_charging_relevance is True
    assert "station_format: Full Service Station" in str(facilities[0].notes)
