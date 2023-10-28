from app.analysis.provider_audit import audit_all_providers, audit_provider, render_provider_audit_markdown


def test_audit_provider_reports_circlek_field_mapping() -> None:
    report = audit_provider("circlek")

    assert report["provider_name"] == "circlek"
    assert report["raw_record_count"] == 1
    assert report["normalized_record_count"] == 1
    assert "site_id" in report["raw_payload_keys"]
    assert "facility_name" in report["normalized_fields_present"]
    assert "name" not in report["dropped_raw_fields"]


def test_audit_all_providers_covers_supported_fixture_corpus() -> None:
    reports = audit_all_providers()

    assert [report["provider_name"] for report in reports] == [
        "circlek",
        "espresso_house",
        "ids",
        "preem",
        "rasta",
        "trafikverket",
        "trb",
    ]


def test_render_provider_audit_markdown_includes_report_sections() -> None:
    markdown = render_provider_audit_markdown([audit_provider("ids")])

    assert "# Provider Audit" in markdown
    assert "## ids" in markdown
    assert "Dropped raw fields" in markdown
