from __future__ import annotations

from app.api import routes
from app.api.dependencies import get_export_service, get_facility_view_service, get_registry, get_repository
from app.core.time import utc_now
from app.normalization.models import NormalizedFacility, RawPayloadRef


def _save_source_facility() -> dict[str, object]:
    repository = get_repository()
    return repository.save_facility(
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


def test_health() -> None:
    assert routes.health() == {"status": "ok"}


def test_readiness_payload_passthrough() -> None:
    payload, status_code = routes.build_readiness_payload()
    assert status_code == 200
    assert payload["status"] == "ok"


def test_providers_endpoint() -> None:
    names = {row["provider"] for row in routes.list_providers(get_registry())}
    assert "circlek" in names
    assert "trafikverket" in names


def test_issues_endpoint_filters_by_provider() -> None:
    repository = get_repository()
    repository.normalization_issues.extend(
        [
            {
                "id": 1,
                "run_id": 2,
                "raw_payload_id": 3,
                "provider_name": "circlek",
                "record_id": "1002",
                "message": "coordinates appeared to be swapped and were corrected",
                "severity": "info",
                "created_at": "2023-09-14T13:36:00",
            },
            {
                "id": 2,
                "run_id": 2,
                "raw_payload_id": 3,
                "provider_name": "preem",
                "record_id": "preem-1",
                "message": "coordinates fell outside Sweden bounds",
                "severity": "warning",
                "created_at": "2023-09-14T13:36:00",
            },
        ]
    )
    body = routes.list_issues(provider="circlek", severity=None, run_id=None, repository=repository)
    assert len(body) == 1
    assert body[0]["provider_name"] == "circlek"


def test_facilities_supports_effective_view_with_override() -> None:
    repository = get_repository()
    source = _save_source_facility()
    repository.upsert_facility_curation(
        int(source["id"]),
        {
            "facility_name": "Circle K Curated",
            "latitude": 59.3333,
            "longitude": 18.05,
            "services": ["toalett", "wifi"],
            "verified_status": "verified",
        },
    )

    body = routes.list_facilities(
        provider=None,
        category=None,
        city=None,
        verified=None,
        view="effective",
        facility_view_service=get_facility_view_service(),
    )
    assert body[0]["facility_name"] == "Circle K Curated"
    assert body[0]["change_status"] == "overridden"
    assert body[0]["verified_status"] == "verified"


def test_map_data_returns_source_and_effective_layers() -> None:
    repository = get_repository()
    source = _save_source_facility()
    repository.upsert_facility_curation(int(source["id"]), {"facility_name": "Circle K Curated"})
    repository.upsert_manual_facility(
        {
            "baserow_row_id": 99,
            "facility_name": "Manual Truck Stop",
            "category": "fuel_station",
            "city": "Malmo",
            "latitude": 55.6,
            "longitude": 13.0,
            "services": ["parking"],
        }
    )

    body = routes.map_data(
        provider=None,
        category=None,
        city=None,
        facility_view_service=get_facility_view_service(),
    )
    assert len(body["source"]) == 1
    assert len(body["effective"]) == 2
    assert body["summary"]["changed_count"] == 1
    assert body["summary"]["manual_count"] == 1
    assert {"id": "fuel", "label": "Fuel", "count": 2} in body["meta"]["need_options"]


def test_map_data_filters_by_customer_need_facets() -> None:
    repository = get_repository()
    repository.save_facility(
        NormalizedFacility(
            provider_name="preem",
            provider_record_id="preem-1",
            source_type="api",
            source_url="https://www.preem.se",
            raw_payload_ref=RawPayloadRef(raw_payload_id=1, provider_name="preem"),
            facility_name="Preem Food",
            facility_brand="Preem",
            category="fuel_station",
            latitude=59.0,
            longitude=18.0,
            services=["foodAndBeverages"],
            fuel_types=["PreemEvolutionDiesel"],
            freshness_ts=utc_now(),
            normalized_hash="preem-food",
        )
    )
    repository.save_facility(
        NormalizedFacility(
            provider_name="ids",
            provider_record_id="ids-1",
            source_type="feed",
            source_url="https://ids.example",
            raw_payload_ref=RawPayloadRef(raw_payload_id=1, provider_name="ids"),
            facility_name="IDS Fuel",
            facility_brand="IDS",
            category="fuel_station",
            latitude=58.0,
            longitude=17.0,
            services=[],
            fuel_types=["diesel"],
            freshness_ts=utc_now(),
            normalized_hash="ids-fuel",
        )
    )

    body = routes.map_data(
        provider=None,
        category=None,
        city=None,
        need="food",
        facility_view_service=get_facility_view_service(),
    )

    assert body["summary"]["effective_count"] == 1
    assert body["effective"][0]["facility_name"] == "Preem Food"
    assert body["effective"][0]["service_facets"] == ["food", "fuel"]
    assert body["effective"][0]["category_label"] == "Fuel station"
    assert "Food & drinks" in body["effective"][0]["source_value_labels"]
    assert "Preem Evolution Diesel" in body["effective"][0]["source_value_labels"]


def test_export_endpoint_returns_effective_bundle() -> None:
    repository = get_repository()
    source = _save_source_facility()
    repository.upsert_facility_curation(int(source["id"]), {"facility_name": "Circle K Curated"})

    body = routes.export_facilities(get_export_service())
    assert body["metadata"]["schema_version"] == "stxtofr.facilities.v1"
    assert body["records"][0]["name"] == "Circle K Curated"
    assert repository.export_builds[0]["status"] == "completed"
