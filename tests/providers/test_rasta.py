from app.providers.rasta.parser import clean_hours, extract_services


def test_extract_services_reads_css_classes() -> None:
    html = '<ul id="ikoner"><li class="restaurang dusch"></li><li class="bransle"></li></ul>'
    assert extract_services(html) == ["bransle", "dusch", "restaurang"]


def test_clean_hours_compacts_whitespace() -> None:
    assert clean_hours("Måndag   06:00 \n 22:00") == "Måndag 06:00 22:00"

