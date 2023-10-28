from app.normalization.geo import is_in_sweden, normalize_coordinates


def test_accepts_valid_swedish_coordinates() -> None:
    result = normalize_coordinates(
        provider_name="circlek",
        record_id="1001",
        latitude=59.3324,
        longitude=18.0586,
    )
    assert result.latitude == 59.3324
    assert result.longitude == 18.0586
    assert not result.issues


def test_swaps_reversed_coordinates_when_swapped_point_is_in_sweden() -> None:
    result = normalize_coordinates(
        provider_name="circlek",
        record_id="1002",
        latitude=18.0586,
        longitude=59.3324,
    )
    assert result.latitude == 59.3324
    assert result.longitude == 18.0586
    assert result.issues
    assert "swapped" in result.issues[0].message


def test_marks_out_of_bounds_coordinates() -> None:
    result = normalize_coordinates(
        provider_name="circlek",
        record_id="1003",
        latitude=40.7128,
        longitude=-74.0060,
    )
    assert result.latitude == 40.7128
    assert result.longitude == -74.006
    assert result.confidence_adjustment < 0
    assert result.issues
    assert not is_in_sweden(result.latitude, result.longitude)

