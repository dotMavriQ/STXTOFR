from __future__ import annotations

from sqlalchemy import JSON, Boolean, Column, DateTime, Float, ForeignKey, Integer, String, Text
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func


Base = declarative_base()


class IngestionRun(Base):
    __tablename__ = "ingestion_runs"

    id = Column(Integer, primary_key=True)
    provider_name = Column(String(64), nullable=False)
    mode = Column(String(32), nullable=False)
    status = Column(String(32), nullable=False)
    dry_run = Column(Boolean, nullable=False, default=False)
    started_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    finished_at = Column(DateTime(timezone=True), nullable=True)
    records_fetched = Column(Integer, nullable=False, default=0)
    records_normalized = Column(Integer, nullable=False, default=0)


class ProviderFetch(Base):
    __tablename__ = "provider_fetches"

    id = Column(Integer, primary_key=True)
    run_id = Column(Integer, ForeignKey("ingestion_runs.id"), nullable=False)
    provider_name = Column(String(64), nullable=False)
    request_url = Column(Text, nullable=False)
    status_code = Column(Integer, nullable=False)
    fetched_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    response_checksum = Column(String(40), nullable=False)


class RawPayload(Base):
    __tablename__ = "raw_payloads"

    id = Column(Integer, primary_key=True)
    provider_name = Column(String(64), nullable=False)
    fetch_id = Column(Integer, ForeignKey("provider_fetches.id"), nullable=True)
    request_url = Column(Text, nullable=False)
    request_headers = Column(JSON, nullable=False, default=dict)
    status_code = Column(Integer, nullable=False)
    fetched_at = Column(DateTime(timezone=True), nullable=False)
    payload = Column(JSON, nullable=False)
    payload_checksum = Column(String(40), nullable=False)
    replay_key = Column(String(128), nullable=False)


class NormalizedFacilityRow(Base):
    __tablename__ = "normalized_facilities"

    id = Column(Integer, primary_key=True)
    provider_name = Column(String(64), nullable=False)
    provider_record_id = Column(String(128), nullable=False)
    source_type = Column(String(32), nullable=False)
    source_url = Column(Text, nullable=True)
    facility_name = Column(String(255), nullable=False)
    facility_brand = Column(String(255), nullable=True)
    category = Column(String(64), nullable=False)
    subcategories = Column(JSON, nullable=False, default=list)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    formatted_address = Column(Text, nullable=True)
    street = Column(String(255), nullable=True)
    city = Column(String(128), nullable=True)
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
    freshness_ts = Column(DateTime(timezone=True), nullable=False)
    normalized_hash = Column(String(40), nullable=False)
    verified_status = Column(String(32), nullable=False, default="unverified")
    notes = Column(Text, nullable=True)


class FacilitySourceLinkRow(Base):
    __tablename__ = "facility_source_links"

    id = Column(Integer, primary_key=True)
    facility_id = Column(Integer, ForeignKey("normalized_facilities.id"), nullable=False)
    provider_name = Column(String(64), nullable=False)
    provider_record_id = Column(String(128), nullable=False)
    raw_payload_id = Column(Integer, ForeignKey("raw_payloads.id"), nullable=False)


class GapFindingRow(Base):
    __tablename__ = "gap_findings"

    id = Column(Integer, primary_key=True)
    finding_type = Column(String(64), nullable=False)
    provider_name = Column(String(64), nullable=True)
    category = Column(String(64), nullable=False)
    region = Column(String(128), nullable=False)
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

