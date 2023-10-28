from pathlib import Path

from app.providers.base import RunContext
from app.providers.rasta.adapter import RastaAdapter
from app.providers.rasta.parser import clean_hours, extract_services, parse_opening_hours_tables, split_swedish_address


FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures"


def test_rasta_fetch_parses_listing_page() -> None:
    class FakeResponse:
        def __init__(self, text: str, status_code: int = 200) -> None:
            self.text = text
            self.status_code = status_code

    class FakeHttpClient:
        def get(self, url: str, **kwargs: object) -> FakeResponse:
            if url == "https://www.rasta.se/anlaggningar/":
                return FakeResponse((FIXTURES_DIR / "rasta_listing.html").read_text())
            if url == "https://www.rasta.se/arboga/":
                return FakeResponse((FIXTURES_DIR / "rasta_detail.html").read_text())
            if url == "https://www.rasta.se/arboga/kontakt/":
                return FakeResponse((FIXTURES_DIR / "rasta_contact.html").read_text())
            raise AssertionError(f"unexpected url {url}")

    adapter = RastaAdapter(http_client=FakeHttpClient())  # type: ignore[arg-type]

    fetch = adapter.fetch(RunContext(mode="full", dry_run=True))

    assert fetch.status_code == 200
    assert fetch.payload["records"][0]["slug"] == "arboga"
    assert fetch.payload["records"][0]["city"] == "Arboga"
    assert fetch.payload["records"][0]["street"] == "Flygvägen 1a"
    assert fetch.payload["records"][0]["postal_code"] == "732 48"
    assert fetch.payload["records"][0]["latitude"] == 59.396361
    assert fetch.payload["records"][0]["phone"] == "0589-101 90, 0589-124 00"
    assert "Restaurang: Måndag - Fredag 06:00 - 22:00" in str(fetch.payload["records"][0]["hours"])
    assert "preem" in fetch.payload["records"][0]["listing_services"]


def test_extract_services_reads_css_classes() -> None:
    html = '<ul id="ikoner"><li class="restaurang dusch"></li><li class="bransle"></li></ul>'
    assert extract_services(html) == ["bransle", "dusch", "restaurang"]


def test_clean_hours_compacts_whitespace() -> None:
    assert clean_hours("Måndag   06:00 \n 22:00") == "Måndag 06:00 22:00"


def test_split_swedish_address_extracts_postal_code_and_city() -> None:
    assert split_swedish_address("Flygvägen 1a, 732 48 Arboga") == ("Flygvägen 1a", "732 48", "Arboga")


def test_parse_opening_hours_tables_collects_sections() -> None:
    html = (FIXTURES_DIR / "rasta_contact.html").read_text()
    parsed = parse_opening_hours_tables(html)
    assert parsed is not None
    assert "Restaurang: Måndag - Fredag 06:00 - 22:00" in parsed
    assert "Butik Preem: Måndag - Fredag 06:00 - 22:00" in parsed
