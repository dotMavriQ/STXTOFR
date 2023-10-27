from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20231025_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "ingestion_runs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("provider_name", sa.String(length=64), nullable=False),
        sa.Column("mode", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("dry_run", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("records_fetched", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("records_normalized", sa.Integer(), nullable=False, server_default="0"),
    )
    op.create_index("ix_ingestion_runs_provider_name", "ingestion_runs", ["provider_name"])
    op.create_index("ix_ingestion_runs_status", "ingestion_runs", ["status"])

    op.create_table(
        "provider_fetches",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("run_id", sa.Integer(), sa.ForeignKey("ingestion_runs.id"), nullable=False),
        sa.Column("provider_name", sa.String(length=64), nullable=False),
        sa.Column("request_url", sa.Text(), nullable=False),
        sa.Column("status_code", sa.Integer(), nullable=False),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("response_checksum", sa.String(length=40), nullable=False),
    )
    op.create_index("ix_provider_fetches_run_id", "provider_fetches", ["run_id"])
    op.create_index("ix_provider_fetches_provider_name", "provider_fetches", ["provider_name"])

    op.create_table(
        "raw_payloads",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("provider_name", sa.String(length=64), nullable=False),
        sa.Column("fetch_id", sa.Integer(), sa.ForeignKey("provider_fetches.id"), nullable=True),
        sa.Column("request_url", sa.Text(), nullable=False),
        sa.Column("request_headers", sa.JSON(), nullable=False),
        sa.Column("status_code", sa.Integer(), nullable=False),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("payload_checksum", sa.String(length=40), nullable=False),
        sa.Column("replay_key", sa.String(length=128), nullable=False),
    )
    op.create_index("ix_raw_payloads_provider_name", "raw_payloads", ["provider_name"])
    op.create_unique_constraint("uq_raw_payloads_replay_key", "raw_payloads", ["replay_key"])

    op.create_table(
        "normalized_facilities",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("provider_name", sa.String(length=64), nullable=False),
        sa.Column("provider_record_id", sa.String(length=128), nullable=False),
        sa.Column("source_type", sa.String(length=32), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=True),
        sa.Column("raw_payload_id", sa.Integer(), sa.ForeignKey("raw_payloads.id"), nullable=False),
        sa.Column("facility_name", sa.String(length=255), nullable=False),
        sa.Column("facility_brand", sa.String(length=255), nullable=True),
        sa.Column("category", sa.String(length=64), nullable=False),
        sa.Column("subcategories", sa.JSON(), nullable=False),
        sa.Column("latitude", sa.Float(), nullable=True),
        sa.Column("longitude", sa.Float(), nullable=True),
        sa.Column("formatted_address", sa.Text(), nullable=True),
        sa.Column("street", sa.String(length=255), nullable=True),
        sa.Column("city", sa.String(length=128), nullable=True),
        sa.Column("region", sa.String(length=128), nullable=True),
        sa.Column("postal_code", sa.String(length=32), nullable=True),
        sa.Column("country_code", sa.String(length=8), nullable=True),
        sa.Column("phone", sa.String(length=64), nullable=True),
        sa.Column("opening_hours", sa.Text(), nullable=True),
        sa.Column("amenities", sa.JSON(), nullable=False),
        sa.Column("services", sa.JSON(), nullable=False),
        sa.Column("fuel_types", sa.JSON(), nullable=False),
        sa.Column("parking_features", sa.JSON(), nullable=False),
        sa.Column("heavy_vehicle_relevance", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("electric_charging_relevance", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("confidence_score", sa.Float(), nullable=False, server_default="0.5"),
        sa.Column("freshness_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("normalized_hash", sa.String(length=40), nullable=False),
        sa.Column("verified_status", sa.String(length=32), nullable=False, server_default="unverified"),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.UniqueConstraint("provider_name", "provider_record_id", name="uq_normalized_facility_provider_record"),
    )
    op.create_index("ix_normalized_facilities_category", "normalized_facilities", ["category"])
    op.create_index("ix_normalized_facilities_city", "normalized_facilities", ["city"])
    op.create_index("ix_normalized_facilities_freshness_ts", "normalized_facilities", ["freshness_ts"])
    op.create_index("ix_normalized_facilities_verified_status", "normalized_facilities", ["verified_status"])
    op.create_index("ix_normalized_facilities_provider_city", "normalized_facilities", ["provider_name", "city"])
    op.create_index("ix_normalized_facilities_provider_category", "normalized_facilities", ["provider_name", "category"])

    op.create_table(
        "normalization_issues",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("run_id", sa.Integer(), sa.ForeignKey("ingestion_runs.id"), nullable=True),
        sa.Column("raw_payload_id", sa.Integer(), sa.ForeignKey("raw_payloads.id"), nullable=True),
        sa.Column("provider_name", sa.String(length=64), nullable=False),
        sa.Column("record_id", sa.String(length=128), nullable=True),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("severity", sa.String(length=32), nullable=False, server_default="warning"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_normalization_issues_provider_name", "normalization_issues", ["provider_name"])
    op.create_index("ix_normalization_issues_run_id", "normalization_issues", ["run_id"])
    op.create_index("ix_normalization_issues_raw_payload_id", "normalization_issues", ["raw_payload_id"])
    op.create_index("ix_normalization_issues_severity", "normalization_issues", ["severity"])

    op.create_table(
        "facility_source_links",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("facility_id", sa.Integer(), sa.ForeignKey("normalized_facilities.id"), nullable=False),
        sa.Column("provider_name", sa.String(length=64), nullable=False),
        sa.Column("provider_record_id", sa.String(length=128), nullable=False),
        sa.Column("facility_hash", sa.String(length=40), nullable=False),
        sa.Column("raw_payload_id", sa.Integer(), sa.ForeignKey("raw_payloads.id"), nullable=False),
    )
    op.create_index("ix_facility_source_links_facility_id", "facility_source_links", ["facility_id"])
    op.create_index("ix_facility_source_links_provider_name", "facility_source_links", ["provider_name"])
    op.create_index("ix_facility_source_links_raw_payload_id", "facility_source_links", ["raw_payload_id"])

    op.create_table(
        "gap_findings",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("finding_type", sa.String(length=64), nullable=False),
        sa.Column("provider_name", sa.String(length=64), nullable=True),
        sa.Column("category", sa.String(length=64), nullable=False),
        sa.Column("region", sa.String(length=128), nullable=False),
        sa.Column("severity", sa.String(length=32), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("facility_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_gap_findings_finding_type", "gap_findings", ["finding_type"])
    op.create_index("ix_gap_findings_provider_name", "gap_findings", ["provider_name"])
    op.create_index("ix_gap_findings_category", "gap_findings", ["category"])
    op.create_index("ix_gap_findings_region", "gap_findings", ["region"])

    op.create_table(
        "provider_checkpoints",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("provider_name", sa.String(length=64), nullable=False),
        sa.Column("checkpoint", sa.String(length=255), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_unique_constraint("uq_provider_checkpoints_provider_name", "provider_checkpoints", ["provider_name"])

    op.create_table(
        "merged_facilities",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("canonical_name", sa.String(length=255), nullable=False),
        sa.Column("category", sa.String(length=64), nullable=False),
        sa.Column("latitude", sa.Float(), nullable=True),
        sa.Column("longitude", sa.Float(), nullable=True),
    )

    op.create_table(
        "merge_candidates",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("left_facility_id", sa.Integer(), nullable=False),
        sa.Column("right_facility_id", sa.Integer(), nullable=False),
        sa.Column("score", sa.Float(), nullable=False),
        sa.Column("reason", sa.String(length=128), nullable=False),
    )

    op.create_table(
        "facility_curations",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("facility_id", sa.Integer(), sa.ForeignKey("normalized_facilities.id"), nullable=False),
        sa.Column("baserow_row_id", sa.Integer(), nullable=True),
        sa.Column("facility_name", sa.String(length=255), nullable=True),
        sa.Column("category", sa.String(length=64), nullable=True),
        sa.Column("formatted_address", sa.Text(), nullable=True),
        sa.Column("street", sa.String(length=255), nullable=True),
        sa.Column("city", sa.String(length=128), nullable=True),
        sa.Column("region", sa.String(length=128), nullable=True),
        sa.Column("postal_code", sa.String(length=32), nullable=True),
        sa.Column("latitude", sa.Float(), nullable=True),
        sa.Column("longitude", sa.Float(), nullable=True),
        sa.Column("phone", sa.String(length=64), nullable=True),
        sa.Column("opening_hours", sa.Text(), nullable=True),
        sa.Column("services", sa.JSON(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("verified_status", sa.String(length=32), nullable=True),
        sa.Column("changed_by", sa.String(length=128), nullable=True),
        sa.Column("source", sa.String(length=32), nullable=False, server_default="baserow"),
        sa.Column("last_pulled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("facility_id", name="uq_facility_curations_facility_id"),
        sa.UniqueConstraint("baserow_row_id", name="uq_facility_curations_baserow_row_id"),
    )

    op.create_table(
        "manual_facilities",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("baserow_row_id", sa.Integer(), nullable=True),
        sa.Column("facility_name", sa.String(length=255), nullable=False),
        sa.Column("facility_brand", sa.String(length=255), nullable=True),
        sa.Column("category", sa.String(length=64), nullable=False),
        sa.Column("formatted_address", sa.Text(), nullable=True),
        sa.Column("street", sa.String(length=255), nullable=True),
        sa.Column("city", sa.String(length=128), nullable=True),
        sa.Column("region", sa.String(length=128), nullable=True),
        sa.Column("postal_code", sa.String(length=32), nullable=True),
        sa.Column("country_code", sa.String(length=8), nullable=True),
        sa.Column("latitude", sa.Float(), nullable=True),
        sa.Column("longitude", sa.Float(), nullable=True),
        sa.Column("phone", sa.String(length=64), nullable=True),
        sa.Column("opening_hours", sa.Text(), nullable=True),
        sa.Column("services", sa.JSON(), nullable=False),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("verified_status", sa.String(length=32), nullable=False, server_default="unverified"),
        sa.Column("source", sa.String(length=32), nullable=False, server_default="baserow"),
        sa.Column("changed_by", sa.String(length=128), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("baserow_row_id", name="uq_manual_facilities_baserow_row_id"),
    )
    op.create_index("ix_manual_facilities_category", "manual_facilities", ["category"])
    op.create_index("ix_manual_facilities_city", "manual_facilities", ["city"])
    op.create_index("ix_manual_facilities_verified_status", "manual_facilities", ["verified_status"])

    op.create_table(
        "curation_sync_runs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("direction", sa.String(length=16), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("pushed_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("pulled_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("updated_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("skipped_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("metadata_json", sa.JSON(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_curation_sync_runs_direction", "curation_sync_runs", ["direction"])
    op.create_index("ix_curation_sync_runs_status", "curation_sync_runs", ["status"])

    op.create_table(
        "export_builds",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("schema_version", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("record_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("metadata_json", sa.JSON(), nullable=False),
        sa.Column("bundle_json", sa.JSON(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_export_builds_status", "export_builds", ["status"])


def downgrade() -> None:
    op.drop_index("ix_export_builds_status", table_name="export_builds")
    op.drop_table("export_builds")
    op.drop_index("ix_curation_sync_runs_status", table_name="curation_sync_runs")
    op.drop_index("ix_curation_sync_runs_direction", table_name="curation_sync_runs")
    op.drop_table("curation_sync_runs")
    op.drop_index("ix_manual_facilities_verified_status", table_name="manual_facilities")
    op.drop_index("ix_manual_facilities_city", table_name="manual_facilities")
    op.drop_index("ix_manual_facilities_category", table_name="manual_facilities")
    op.drop_table("manual_facilities")
    op.drop_table("facility_curations")
    op.drop_table("merge_candidates")
    op.drop_table("merged_facilities")
    op.drop_constraint("uq_provider_checkpoints_provider_name", "provider_checkpoints", type_="unique")
    op.drop_table("provider_checkpoints")
    op.drop_index("ix_gap_findings_region", table_name="gap_findings")
    op.drop_index("ix_gap_findings_category", table_name="gap_findings")
    op.drop_index("ix_gap_findings_provider_name", table_name="gap_findings")
    op.drop_index("ix_gap_findings_finding_type", table_name="gap_findings")
    op.drop_table("gap_findings")
    op.drop_index("ix_facility_source_links_raw_payload_id", table_name="facility_source_links")
    op.drop_index("ix_facility_source_links_provider_name", table_name="facility_source_links")
    op.drop_index("ix_facility_source_links_facility_id", table_name="facility_source_links")
    op.drop_table("facility_source_links")
    op.drop_index("ix_normalization_issues_severity", table_name="normalization_issues")
    op.drop_index("ix_normalization_issues_raw_payload_id", table_name="normalization_issues")
    op.drop_index("ix_normalization_issues_run_id", table_name="normalization_issues")
    op.drop_index("ix_normalization_issues_provider_name", table_name="normalization_issues")
    op.drop_table("normalization_issues")
    op.drop_index("ix_normalized_facilities_provider_category", table_name="normalized_facilities")
    op.drop_index("ix_normalized_facilities_provider_city", table_name="normalized_facilities")
    op.drop_index("ix_normalized_facilities_verified_status", table_name="normalized_facilities")
    op.drop_index("ix_normalized_facilities_freshness_ts", table_name="normalized_facilities")
    op.drop_index("ix_normalized_facilities_city", table_name="normalized_facilities")
    op.drop_index("ix_normalized_facilities_category", table_name="normalized_facilities")
    op.drop_table("normalized_facilities")
    op.drop_constraint("uq_raw_payloads_replay_key", "raw_payloads", type_="unique")
    op.drop_index("ix_raw_payloads_provider_name", table_name="raw_payloads")
    op.drop_table("raw_payloads")
    op.drop_index("ix_provider_fetches_provider_name", table_name="provider_fetches")
    op.drop_index("ix_provider_fetches_run_id", table_name="provider_fetches")
    op.drop_table("provider_fetches")
    op.drop_index("ix_ingestion_runs_status", table_name="ingestion_runs")
    op.drop_index("ix_ingestion_runs_provider_name", table_name="ingestion_runs")
    op.drop_table("ingestion_runs")
