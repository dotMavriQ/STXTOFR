from __future__ import annotations

import re
from typing import Any


SERVICE_FACETS: tuple[dict[str, str], ...] = (
    {"id": "food", "label": "Food & drinks"},
    {"id": "fuel", "label": "Fuel"},
    {"id": "charging", "label": "EV charging"},
    {"id": "parking", "label": "Parking"},
    {"id": "truck", "label": "Truck ready"},
    {"id": "restrooms", "label": "Restrooms & showers"},
    {"id": "shop", "label": "Shop"},
    {"id": "car_care", "label": "Car wash & air"},
    {"id": "family_access", "label": "Family & accessibility"},
    {"id": "lodging", "label": "Lodging"},
)

FACET_LABELS = {facet["id"]: facet["label"] for facet in SERVICE_FACETS}

CATEGORY_LABELS = {
    "fuel_station": "Fuel station",
    "roadside_rest": "Roadside stop",
    "rest_area": "Rest area",
    "parking": "Parking",
    "coffee_shop": "Coffee shop",
}

VALUE_LABELS = {
    "adapted_for_24m_vehicles": "Adapted for 24 m vehicles",
    "adblue": "AdBlue",
    "air_water": "Air & water",
    "baby_changing": "Baby changing",
    "bensin": "Bensin",
    "car_wash": "Car wash",
    "carwashautomatic": "Automatic car wash",
    "carwashselfservice": "Self-service car wash",
    "child_friendly": "Child friendly",
    "diesel": "Diesel",
    "dusch": "Shower",
    "e85": "E85",
    "evolutionbensin95": "Evolution Bensin 95",
    "evolutiondiesel": "Evolution Diesel",
    "express_checkout": "Express checkout",
    "fast_charging": "Fast charging",
    "food": "Food",
    "foodandbeverages": "Food & drinks",
    "hvo100": "HVO100",
    "lastbilsparkering": "Truck parking",
    "limited_inventory": "Limited inventory",
    "mobile_payment_fuel": "Mobile fuel payment",
    "mobile_payment_fuel_business": "Business mobile fuel payment",
    "parking": "Parking",
    "petrol_95": "Petrol 95",
    "petrol_98": "Petrol 98",
    "preemadblue": "Preem AdBlue",
    "preemevolutionbensin95": "Preem Evolution Bensin 95",
    "preemevolutiondiesel": "Preem Evolution Diesel",
    "preemhvo100": "Preem HVO100",
    "preorder": "Preorder",
    "restaurant": "Restaurant",
    "restaurang": "Restaurant",
    "saifa_connected": "Såifa connected",
    "self_service_car_wash": "Self-service car wash",
    "shop": "Shop",
    "storefood": "Store & food",
    "store_food": "Store & food",
    "storenfood": "Store & food",
    "store_n_food": "Store & food",
    "takeaway_only": "Takeaway only",
    "toalett": "Toilet",
    "toilet": "Toilet",
    "trailer_rental": "Trailer rental",
    "truck_diesel": "Truck diesel",
    "truck_parking": "Truck parking",
    "truck_stop": "Truck stop",
    "vacuum": "Vacuum",
    "windshield_fluid": "Windshield fluid",
}

FACET_MATCHERS = {
    "food": {
        "categories": {"coffee_shop", "roadside_rest"},
        "tokens": {
            "cafe",
            "coffee",
            "coffeeshop",
            "coffee_shop",
            "food",
            "foodandbeverages",
            "food_and_beverages",
            "restaurant",
            "restaurang",
            "storefood",
            "store_food",
            "storenfood",
            "store_n_food",
            "takeaway",
            "takeaway_only",
        },
    },
    "fuel": {
        "categories": {"fuel_station"},
        "tokens": {
            "adblue",
            "bensin",
            "cng",
            "diesel",
            "e85",
            "fuel",
            "fuel_station",
            "hvo100",
            "lbg",
            "lbg50",
            "lng",
            "petrol_95",
            "petrol_98",
            "truck_diesel",
        },
    },
    "charging": {
        "tokens": {
            "charging",
            "electric_charging",
            "electricchargingstation",
            "ev_charger",
            "fast_charging",
            "high_speed_charger",
            "snabbladdning",
        },
    },
    "parking": {
        "categories": {"parking"},
        "tokens": {
            "lastbilsparkering",
            "lorry",
            "parking",
            "truck_parking",
            "truckparking",
        },
    },
    "truck": {
        "tokens": {
            "24m",
            "adapted_for_24m_vehicles",
            "heavy_vehicle",
            "lastbil",
            "lorry",
            "truck",
            "truck_diesel",
            "truck_parking",
            "truck_stop",
        },
    },
    "restrooms": {
        "tokens": {
            "baby_changing",
            "dusch",
            "restroom",
            "shower",
            "toalett",
            "toilet",
            "toilets",
            "wc",
        },
    },
    "shop": {
        "tokens": {
            "butik",
            "convenience",
            "convenience_store",
            "kiosk",
            "limited_inventory",
            "shop",
            "store",
            "storefood",
            "store_food",
            "storenfood",
            "store_n_food",
        },
    },
    "car_care": {
        "tokens": {
            "air_water",
            "car_wash",
            "carwash",
            "carwashautomatic",
            "carwashselfservice",
            "self_service_car_wash",
            "service_zone",
            "vacuum",
            "windshield_fluid",
        },
    },
    "family_access": {
        "tokens": {
            "accessible",
            "baby_changing",
            "barnvanlig",
            "child_friendly",
            "handicapfriendly",
            "handicap_friendly",
        },
    },
    "lodging": {
        "tokens": {
            "hotel",
            "lodging",
            "motel",
            "room",
            "rooms",
        },
    },
}


def normalize_taxonomy_token(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value).strip().lower()).strip("_")


def compact_taxonomy_token(value: Any) -> str:
    return normalize_taxonomy_token(value).replace("_", "")


def build_service_facets(row: dict[str, Any]) -> list[str]:
    category = normalize_taxonomy_token(row.get("category") or "")
    tokens = _row_tokens(row)
    facets: list[str] = []
    for facet in SERVICE_FACETS:
        facet_id = facet["id"]
        matcher = FACET_MATCHERS.get(facet_id, {})
        categories = set(matcher.get("categories", set()))
        match_tokens = set(matcher.get("tokens", set()))
        compact_match_tokens = {compact_taxonomy_token(token) for token in match_tokens}
        matched = category in categories
        matched = matched or bool(tokens & match_tokens)
        matched = matched or bool({compact_taxonomy_token(token) for token in tokens} & compact_match_tokens)
        if facet_id == "charging":
            matched = matched or bool(row.get("electric_charging_relevance"))
        if facet_id == "truck":
            matched = matched or bool(row.get("heavy_vehicle_relevance"))
        if matched:
            facets.append(facet_id)
    return facets


def build_service_facet_labels(facets: list[str]) -> list[str]:
    return [FACET_LABELS[facet] for facet in facets if facet in FACET_LABELS]


def build_category_label(category: Any) -> str:
    normalized = normalize_taxonomy_token(category or "")
    if not normalized:
        return "Uncategorized"
    return CATEGORY_LABELS.get(normalized, str(category).replace("_", " ").title())


def build_value_label(value: Any) -> str:
    if value is None:
        return ""
    raw = str(value).strip()
    if not raw:
        return ""
    normalized = normalize_taxonomy_token(raw)
    compact = compact_taxonomy_token(raw)
    if normalized in VALUE_LABELS:
        return VALUE_LABELS[normalized]
    if compact in VALUE_LABELS:
        return VALUE_LABELS[compact]
    return _titleize_value(raw)


def build_value_labels(values: list[Any]) -> list[str]:
    labels: list[str] = []
    seen: set[str] = set()
    for value in values:
        label = build_value_label(value)
        if not label or label.casefold() in seen:
            continue
        labels.append(label)
        seen.add(label.casefold())
    return labels


def service_facet_options() -> list[dict[str, str]]:
    return list(SERVICE_FACETS)


def _row_tokens(row: dict[str, Any]) -> set[str]:
    values: list[Any] = [
        row.get("category"),
        *(row.get("subcategories") or []),
        *(row.get("services") or []),
        *(row.get("amenities") or []),
        *(row.get("fuel_types") or []),
        *(row.get("parking_features") or []),
    ]
    tokens = {normalize_taxonomy_token(value) for value in values if str(value or "").strip()}
    tokens.update(compact_taxonomy_token(value) for value in values if str(value or "").strip())
    return tokens


def _titleize_value(value: str) -> str:
    spaced = re.sub(r"([a-zåäö])([A-ZÅÄÖ])", r"\1 \2", value)
    spaced = re.sub(r"([A-Za-zÅÄÖåäö])([0-9])", r"\1 \2", spaced)
    spaced = re.sub(r"([0-9])([A-Za-zÅÄÖåäö])", r"\1 \2", spaced)
    spaced = re.sub(r"[_/\\-]+", " ", spaced)
    spaced = re.sub(r"\s+", " ", spaced).strip()
    tokens = []
    for token in spaced.split(" "):
        lower = token.lower()
        if lower in {"hvo100", "lng", "cng", "lbg", "lpg", "e85"}:
            tokens.append(lower.upper())
        elif lower == "adblue":
            tokens.append("AdBlue")
        elif token.isupper() and len(token) <= 4:
            tokens.append(token)
        else:
            tokens.append(token[:1].upper() + token[1:])
    return " ".join(tokens)
