from __future__ import annotations

from sqlalchemy import JSON, Boolean, Column, DateTime, Float, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func


Base = declarative_base()


class IngestionRun(Base):
    __tablename__ = "ingestion_runs"

    id = Column(Integer, primary_key=True)
    provider_name = Column(String(64), nullable=False, index=True)
    mode = Column(String(32), nullable=False)
    status = Column(String(32), nullable=False, index=True)
    dry_run = Column(Boolean, nullable=False, default=False)
    started_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    finished_at = Column(DateTime(timezone=True), nullable=True)
    records_fetched = Column(Integer, nullable=False, default=0)
    records_normalized = Column(Integer, nullable=False, default=0)


class ProviderFetch(Base):
    __tablename__ = "provider_fetches"

    id = Column(Integer, primary_key=True)
    run_id = Column(Integer, ForeignKey("ingestion_runs.id"), nullable=False, index=True)
    provider_name = Column(String(64), nullable=False, index=True)
    request_url = Column(Text, nullable=False)
    status_code = Column(Integer, nullable=False)
    fetched_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    response_checksum = Column(String(40), nullable=False)


class RawPayload(Base):
    __tablename__ = "raw_payloads"

    id = Column(Integer, primary_key=True)
    provider_name = Column(String(64), nullable=False, index=True)
    fetch_id = Column(Integer, ForeignKey("provider_fetches.id"), nullable=True)
    request_url = Column(Text, nullable=False)
    request_headers = Column(JSON, nullable=False, default=dict)
    status_code = Column(Integer, nullable=False)
    fetched_at = Column(DateTime(timezone=True), nullable=False)
    payload = Column(JSON, nullable=False)
    payload_checksum = Column(String(40), nullable=False)
    replay_key = Column(String(128), nullable=False, unique=True)


class NormalizedFacilityRow(Base):
    __tablename__ = "normalized_facilities"

    id = Column(Integer, primary_key=True)
    provider_name = Column(String(64), nullable=False)
    provider_record_id = Column(String(128), nullable=False)
    source_type = Column(String(32), nullable=False)
    source_url = Column(Text, nullable=True)
    raw_payload_id = Column(Integer, ForeignKey("raw_payloads.id"), nullable=False)
    facility_name = Column(String(255), nullable=False)
    facility_brand = Column(String(255), nullable=True)
    category = Column(String(64), nullable=False, index=True)
    subcategories = Column(JSON, nullable=False, default=list)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    formatted_address = Column(Text, nullable=True)
    street = Column(String(255), nullable=True)
    city = Column(String(128), nullable=True, index=True)
    region = Column(String(128), nullable=True)
    postal_code = Column(String(32), nullable=True)
    country_code = Column(String(8), nullable=True)
    phone = Column(String(64), nullable=True)
    opening_hours = Column(Text, nullable=True)
    amenities = Column(JSON, nullable=False, default=list)
    services = Column(JSON, nullable=False, default=list)
    fuel_types = Column(JSON, nullable=False, default=list)
    parking_features = Column(JSON, nullable=False, default=list)
    heavy_vehicle_relevance = Column(Boolean, nullable=False, default=False)
    electric_charging_relevance = Column(Boolean, nullable=False, default=False)
    confidence_score = Column(Float, nullable=False, default=0.5)
    freshness_ts = Column(DateTime(timezone=True), nullable=False, index=True)
    normalized_hash = Column(String(40), nullable=False)
    verified_status = Column(String(32), nullable=False, default="unverified", index=True)
    notes = Column(Text, nullable=True)

    __table_args__ = (
        UniqueConstraint("provider_name", "provider_record_id", name="uq_normalized_facility_provider_record"),
        Index("ix_normalized_facilities_provider_city", "provider_name", "city"),
        Index("ix_normalized_facilities_provider_category", "provider_name", "category"),
    )


class NormalizationIssueRow(Base):
    __tablename__ = "normalization_issues"

    id = Column(Integer, primary_key=True)
    run_id = Column(Integer, ForeignKey("ingestion_runs.id"), nullable=True, index=True)
    raw_payload_id = Column(Integer, ForeignKey("raw_payloads.id"), nullable=True, index=True)
    provider_name = Column(String(64), nullable=False, index=True)
    record_id = Column(String(128), nullable=True)
    message = Column(Text, nullable=False)
    severity = Column(String(32), nullable=False, default="warning", index=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())


class FacilitySourceLinkRow(Base):
    __tablename__ = "facility_source_links"

    id = Column(Integer, primary_key=True)
    facility_id = Column(Integer, ForeignKey("normalized_facilities.id"), nullable=False, index=True)
    provider_name = Column(String(64), nullable=False, index=True)
    provider_record_id = Column(String(128), nullable=False)
    facility_hash = Column(String(40), nullable=False)
    raw_payload_id = Column(Integer, ForeignKey("raw_payloads.id"), nullable=False, index=True)


class GapFindingRow(Base):
    __tablename__ = "gap_findings"

    id = Column(Integer, primary_key=True)
    finding_type = Column(String(64), nullable=False, index=True)
    provider_name = Column(String(64), nullable=True, index=True)
    category = Column(String(64), nullable=False, index=True)
    region = Column(String(128), nullable=False, index=True)
    severity = Column(String(32), nullable=False)
    message = Column(Text, nullable=False)
    facility_id = Column(Integer, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())


class ProviderCheckpoint(Base):
    __tablename__ = "provider_checkpoints"

    id = Column(Integer, primary_key=True)
    provider_name = Column(String(64), nullable=False, unique=True)
    checkpoint = Column(String(255), nullable=True)
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())


class MergedFacilityRow(Base):
    __tablename__ = "merged_facilities"

    id = Column(Integer, primary_key=True)
    canonical_name = Column(String(255), nullable=False)
    category = Column(String(64), nullable=False)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)


class MergeCandidateRow(Base):
    __tablename__ = "merge_candidates"

    id = Column(Integer, primary_key=True)
    left_facility_id = Column(Integer, nullable=False)
    right_facility_id = Column(Integer, nullable=False)
    score = Column(Float, nullable=False)
    reason = Column(String(128), nullable=False)


class FacilityCurationRow(Base):
    __tablename__ = "facility_curations"

    id = Column(Integer, primary_key=True)
    facility_id = Column(Integer, ForeignKey("normalized_facilities.id"), nullable=False, unique=True)
    baserow_row_id = Column(Integer, nullable=True, unique=True)
    facility_name = Column(String(255), nullable=True)
    category = Column(String(64), nullable=True)
    formatted_address = Column(Text, nullable=True)
    street = Column(String(255), nullable=True)
    city = Column(String(128), nullable=True)
    region = Column(String(128), nullable=True)
    postal_code = Column(String(32), nullable=True)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    phone = Column(String(64), nullable=True)
    opening_hours = Column(Text, nullable=True)
    services = Column(JSON, nullable=True)
    notes = Column(Text, nullable=True)
    verified_status = Column(String(32), nullable=True)
    changed_by = Column(String(128), nullable=True)
    source = Column(String(32), nullable=False, default="baserow")
    last_pulled_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())


class ManualFacilityRow(Base):
    __tablename__ = "manual_facilities"

    id = Column(Integer, primary_key=True)
    baserow_row_id = Column(Integer, nullable=True, unique=True)
    facility_name = Column(String(255), nullable=False)
    facility_brand = Column(String(255), nullable=True)
    category = Column(String(64), nullable=False, index=True)
    formatted_address = Column(Text, nullable=True)
    street = Column(String(255), nullable=True)
    city = Column(String(128), nullable=True, index=True)
    region = Column(String(128), nullable=True)
    postal_code = Column(String(32), nullable=True)
    country_code = Column(String(8), nullable=True, default="se")
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    phone = Column(String(64), nullable=True)
    opening_hours = Column(Text, nullable=True)
    services = Column(JSON, nullable=False, default=list)
    notes = Column(Text, nullable=True)
    verified_status = Column(String(32), nullable=False, default="unverified", index=True)
    source = Column(String(32), nullable=False, default="baserow")
    changed_by = Column(String(128), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())


class CurationSyncRunRow(Base):
    __tablename__ = "curation_sync_runs"

    id = Column(Integer, primary_key=True)
    direction = Column(String(16), nullable=False, index=True)
    status = Column(String(32), nullable=False, index=True)
    pushed_count = Column(Integer, nullable=False, default=0)
    pulled_count = Column(Integer, nullable=False, default=0)
    created_count = Column(Integer, nullable=False, default=0)
    updated_count = Column(Integer, nullable=False, default=0)
    skipped_count = Column(Integer, nullable=False, default=0)
    error_message = Column(Text, nullable=True)
    metadata_json = Column(JSON, nullable=False, default=dict)
    started_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    finished_at = Column(DateTime(timezone=True), nullable=True)


class ExportBuildRow(Base):
    __tablename__ = "export_builds"

    id = Column(Integer, primary_key=True)
    schema_version = Column(String(64), nullable=False)
    status = Column(String(32), nullable=False, index=True)
    record_count = Column(Integer, nullable=False, default=0)
    metadata_json = Column(JSON, nullable=False, default=dict)
    bundle_json = Column(JSON, nullable=True)
    error_message = Column(Text, nullable=True)
    started_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    finished_at = Column(DateTime(timezone=True), nullable=True)
