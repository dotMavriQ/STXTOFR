from __future__ import annotations

import re
from bs4 import BeautifulSoup


def extract_services(html: str) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    services: list[str] = []
    for item in soup.select("ul#ikoner li"):
        services.extend(item.get("class", []))
    return sorted(set(services))


def clean_hours(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = re.sub(r"\s+", " ", value).strip()
    return cleaned or None

