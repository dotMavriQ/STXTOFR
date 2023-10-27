from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

import requests

from app.core.config import get_settings


BASEROW_REVIEW_FIELDS: tuple[dict[str, Any], ...] = (
    {"name": "stxtofr_key", "type": "text"},
    {"name": "row_origin", "type": "text"},
    {"name": "source_facility_id", "type": "number", "number_decimal_places": 0, "number_negative": False},
    {"name": "provider_name", "type": "text"},
    {"name": "provider_record_id", "type": "text"},
    {"name": "facility_brand", "type": "text"},
    {"name": "source_type", "type": "text"},
    {"name": "source_url", "type": "url"},
    {"name": "facility_name", "type": "text"},
    {"name": "category", "type": "text"},
    {"name": "formatted_address", "type": "long_text"},
    {"name": "street", "type": "text"},
    {"name": "city", "type": "text"},
    {"name": "region", "type": "text"},
    {"name": "postal_code", "type": "text"},
    {"name": "country_code", "type": "text", "text_default": "se"},
    {"name": "latitude", "type": "number", "number_decimal_places": 7, "number_negative": True},
    {"name": "longitude", "type": "number", "number_decimal_places": 7, "number_negative": True},
    {"name": "phone", "type": "text"},
    {"name": "opening_hours", "type": "long_text"},
    {"name": "services", "type": "long_text"},
    {"name": "notes", "type": "long_text"},
    {"name": "verified_status", "type": "text", "text_default": "unverified"},
)


class BaserowClient(Protocol):
    def list_rows(self) -> list[dict[str, Any]]: ...
    def create_row(self, fields: dict[str, Any]) -> dict[str, Any]: ...
    def update_row(self, row_id: int, fields: dict[str, Any]) -> dict[str, Any]: ...
    def ensure_review_schema(self) -> dict[str, Any]: ...


class DisabledBaserowClient:
    def list_rows(self) -> list[dict[str, Any]]:
        raise RuntimeError("Baserow integration is not configured")

    def create_row(self, fields: dict[str, Any]) -> dict[str, Any]:
        raise RuntimeError("Baserow integration is not configured")

    def update_row(self, row_id: int, fields: dict[str, Any]) -> dict[str, Any]:
        raise RuntimeError("Baserow integration is not configured")

    def ensure_review_schema(self) -> dict[str, Any]:
        raise RuntimeError("Baserow integration is not configured")


@dataclass
class HttpBaserowClient:
    base_url: str
    token: str
    table_id: str
    admin_email: str = ""
    admin_password: str = ""
    timeout_seconds: int = 20

    def list_rows(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        page = 1
        while True:
            response = requests.get(
                self._rows_url(),
                headers=self._headers(),
                params={
                    "user_field_names": "true",
                    "page": str(page),
                    "size": "200",
                },
                timeout=self.timeout_seconds,
            )
            response.raise_for_status()
            payload = response.json()
            rows.extend(list(payload.get("results") or []))
            if not payload.get("next"):
                break
            page += 1
        return rows

    def create_row(self, fields: dict[str, Any]) -> dict[str, Any]:
        response = requests.post(
            self._rows_url(),
            headers=self._headers(),
            params={"user_field_names": "true"},
            json=fields,
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        return response.json()

    def update_row(self, row_id: int, fields: dict[str, Any]) -> dict[str, Any]:
        response = requests.patch(
            f"{self._rows_url()}{row_id}/",
            headers=self._headers(),
            params={"user_field_names": "true"},
            json=fields,
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        return response.json()

    def ensure_review_schema(self) -> dict[str, Any]:
        existing_fields = self._list_fields()
        existing_by_name = {field.get("name"): field for field in existing_fields}
        created: list[dict[str, Any]] = []
        existing: list[dict[str, Any]] = []
        for definition in BASEROW_REVIEW_FIELDS:
            if definition["name"] in existing_by_name:
                existing.append(existing_by_name[definition["name"]])
                continue
            created.append(self._create_field(definition))
        return {
            "status": "completed",
            "table_id": self.table_id,
            "required_count": len(BASEROW_REVIEW_FIELDS),
            "existing_count": len(existing),
            "created_count": len(created),
            "created_fields": [field["name"] for field in created],
            "existing_fields": [field["name"] for field in existing],
        }

    def _rows_url(self) -> str:
        return f"{self.base_url.rstrip('/')}/api/database/rows/table/{self.table_id}/"

    def _fields_url(self) -> str:
        return f"{self.base_url.rstrip('/')}/api/database/fields/table/{self.table_id}/"

    def _list_fields(self) -> list[dict[str, Any]]:
        response = requests.get(
            self._fields_url(),
            headers=self._schema_headers(),
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        return list(response.json() or [])

    def _create_field(self, definition: dict[str, Any]) -> dict[str, Any]:
        response = requests.post(
            self._fields_url(),
            headers=self._schema_headers(),
            json=definition,
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        return response.json()

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Token {self.token}",
            "Content-Type": "application/json",
        }

    def _schema_headers(self) -> dict[str, str]:
        if self.admin_email and self.admin_password:
            return {
                "Authorization": f"JWT {self._admin_jwt()}",
                "Content-Type": "application/json",
            }
        return self._headers()

    def _admin_jwt(self) -> str:
        response = requests.post(
            f"{self.base_url.rstrip('/')}/api/user/token-auth/",
            headers={"Content-Type": "application/json"},
            json={"email": self.admin_email, "password": self.admin_password},
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        token = payload.get("access_token") or payload.get("token")
        if not token:
            raise RuntimeError("Baserow admin authentication did not return an access token")
        return str(token)


def build_baserow_client() -> BaserowClient:
    settings = get_settings()
    if settings.baserow_backend != "api":
        return DisabledBaserowClient()
    if not settings.baserow_url or not settings.baserow_token or not settings.baserow_table_id:
        return DisabledBaserowClient()
    return HttpBaserowClient(
        base_url=settings.baserow_url,
        token=settings.baserow_token,
        table_id=settings.baserow_table_id,
        admin_email=settings.baserow_admin_email,
        admin_password=settings.baserow_admin_password,
    )
