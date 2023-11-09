from __future__ import annotations

import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup


def extract_services(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    services: list[str] = []
    for item in soup.select("ul#ikoner li"):
        services.extend(item.get("class", []))
    return sorted(set(services))


def clean_hours(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = re.sub(r"\s+", " ", value).strip()
    return cleaned or None


def split_swedish_address(value: str | None) -> tuple[str | None, str | None, str | None]:
    if not value:
        return None, None, None
    cleaned = clean_hours(value)
    if not cleaned:
        return None, None, None
    postal_city_match = re.fullmatch(r"(?P<postal>\d{3}\s?\d{2})\s+(?P<city>.+)", cleaned)
    if postal_city_match is not None:
        postal_code = re.sub(r"\s+", " ", postal_city_match.group("postal")).strip()
        city = clean_hours(postal_city_match.group("city"))
        return None, postal_code, city
    match = re.search(r"(?P<street>.+?),?\s+(?P<postal>\d{3}\s?\d{2})\s+(?P<city>.+)$", cleaned)
    if match is None:
        return cleaned, None, None
    street = clean_hours(match.group("street"))
    postal_code = re.sub(r"\s+", " ", match.group("postal")).strip()
    city = clean_hours(match.group("city"))
    return street, postal_code, city


def extract_listing_services(class_names: list[str]) -> list[str]:
    ignored = {"anlaggning", "vagkrog"}
    return sorted({value.strip().lower() for value in class_names if value.strip() and value not in ignored})


def parse_opening_hours_tables(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    wrap = soup.select_one("div.opening-hours-wrap")
    if wrap is None:
        return None
    parts: list[str] = []
    current_section: str | None = None
    for element in wrap.find_all(["h2", "h3", "table"]):
        if element.name == "h2":
            continue
        if element.name == "h3":
            current_section = clean_hours(element.get_text(" ", strip=True))
            continue
        rows: list[str] = []
        for row in element.select("tr"):
            columns = [clean_hours(cell.get_text(" ", strip=True)) for cell in row.select("td")]
            columns = [value for value in columns if value]
            if len(columns) >= 2:
                rows.append(f"{columns[0]} {columns[1]}")
        if rows:
            label = current_section or "Hours"
            parts.append(f"{label}: {'; '.join(rows)}")
    return " | ".join(parts) or None


def extract_marker_coordinates(html: str) -> tuple[float | None, float | None]:
    soup = BeautifulSoup(html, "html.parser")
    marker = soup.select_one("div.acf-map div.marker[data-lat][data-lng]")
    if marker is None:
        return None, None
    lat = marker.get("data-lat")
    lng = marker.get("data-lng")
    if not lat or not lng:
        return None, None
    return float(lat), float(lng)


def extract_contact_url(html: str, base_url: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    for anchor in soup.select("nav#menu-subsite a"):
        href = anchor.get("href")
        label = clean_hours(anchor.get_text(" ", strip=True))
        if href and label and "kontakt" in label.lower():
            return urljoin(base_url, href)
    return None
