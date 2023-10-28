from __future__ import annotations

from app.normalization.taxonomy import build_category_label, build_service_facets, build_value_label, build_value_labels


def test_food_provider_vocabularies_share_food_facet() -> None:
    preem_row = {
        "category": "fuel_station",
        "services": ["foodAndBeverages"],
        "fuel_types": ["diesel"],
    }
    circlek_row = {
        "category": "fuel_station",
        "services": ["storenfood"],
        "fuel_types": ["diesel"],
    }

    assert "food" in build_service_facets(preem_row)
    assert "food" in build_service_facets(circlek_row)
    assert "fuel" in build_service_facets(preem_row)
    assert "fuel" in build_service_facets(circlek_row)


def test_category_labels_are_customer_readable() -> None:
    assert build_category_label("fuel_station") == "Fuel station"
    assert build_category_label("coffee_shop") == "Coffee shop"


def test_provider_values_are_customer_readable() -> None:
    assert build_value_label("PreemEvolutionDiesel") == "Preem Evolution Diesel"
    assert build_value_label("EvolutionBensin95") == "Evolution Bensin 95"
    assert build_value_label("foodAndBeverages") == "Food & drinks"
    assert build_value_label("storenfood") == "Store & food"
    assert build_value_label("petrol_95") == "Petrol 95"


def test_value_labels_dedupe_without_losing_order() -> None:
    assert build_value_labels(["diesel", "Diesel", "HVO100"]) == ["Diesel", "HVO100"]
