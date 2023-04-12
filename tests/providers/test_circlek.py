from app.providers.circlek.adapter import CircleKAdapter


def test_circlek_normalize_maps_station_name() -> None:
    adapter = CircleKAdapter()
    fetch_result = adapter.fetch(run_context=None)  # type: ignore[arg-type]
    facilities, issues = adapter.normalize(fetch_result.payload, fetched_at=fetch_result.fetched_at)
    assert not issues
    assert facilities[0].facility_name == "Circle K Arlandastad"
    assert "diesel" in facilities[0].fuel_types

