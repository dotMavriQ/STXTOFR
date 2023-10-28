from __future__ import annotations

from typing import Any

from app.services.baserow import HttpBaserowClient


class _FakeResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, Any]:
        return self._payload


def test_http_baserow_list_rows_follows_pagination(monkeypatch) -> None:
    calls: list[dict[str, Any]] = []

    def fake_get(url: str, headers: dict[str, str], params: dict[str, str], timeout: int) -> _FakeResponse:
        calls.append({"url": url, "headers": headers, "params": params, "timeout": timeout})
        page = int(params["page"])
        if page == 1:
            return _FakeResponse(
                {
                    "next": "http://example.test/api/database/rows/table/603/?page=2&size=200&user_field_names=true",
                    "results": [{"id": 1}, {"id": 2}],
                }
            )
        if page == 2:
            return _FakeResponse(
                {
                    "next": None,
                    "results": [{"id": 3}],
                }
            )
        raise AssertionError("unexpected page")

    monkeypatch.setattr("app.services.baserow.requests.get", fake_get)

    client = HttpBaserowClient(base_url="http://localhost:8080", token="token", table_id="603")

    rows = client.list_rows()

    assert rows == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert [call["params"]["page"] for call in calls] == ["1", "2"]


def test_http_baserow_ensure_review_schema_creates_missing_fields(monkeypatch) -> None:
    get_calls: list[dict[str, Any]] = []
    post_calls: list[dict[str, Any]] = []

    def fake_get(url: str, headers: dict[str, str], timeout: int) -> _FakeResponse:
        get_calls.append({"url": url, "headers": headers, "timeout": timeout})
        return _FakeResponse(
            [
                {"id": 1, "name": "stxtofr_key", "type": "text"},
                {"id": 2, "name": "row_origin", "type": "text"},
            ]
        )

    def fake_post(url: str, headers: dict[str, str], json: dict[str, Any], timeout: int) -> _FakeResponse:
        post_calls.append({"url": url, "headers": headers, "json": json, "timeout": timeout})
        return _FakeResponse({"id": len(post_calls) + 2, **json})

    monkeypatch.setattr("app.services.baserow.requests.get", fake_get)
    monkeypatch.setattr("app.services.baserow.requests.post", fake_post)

    client = HttpBaserowClient(base_url="http://localhost:8080", token="token", table_id="603")

    result = client.ensure_review_schema()

    assert result["status"] == "completed"
    assert result["existing_count"] == 2
    assert result["created_count"] == 21
    assert "facility_name" in result["created_fields"]
    assert post_calls[0]["json"] == {
        "name": "source_facility_id",
        "type": "number",
        "number_decimal_places": 0,
        "number_negative": False,
    }
    assert get_calls[0]["url"].endswith("/api/database/fields/table/603/")


def test_http_baserow_ensure_review_schema_can_use_admin_jwt(monkeypatch) -> None:
    headers_seen: list[dict[str, str]] = []

    def fake_post(url: str, headers: dict[str, str], json: dict[str, Any], timeout: int) -> _FakeResponse:
        if url.endswith("/api/user/token-auth/"):
            return _FakeResponse({"access_token": "jwt-token"})
        headers_seen.append(headers)
        return _FakeResponse({"id": 1, **json})

    def fake_get(url: str, headers: dict[str, str], timeout: int) -> _FakeResponse:
        headers_seen.append(headers)
        return _FakeResponse([])

    monkeypatch.setattr("app.services.baserow.requests.get", fake_get)
    monkeypatch.setattr("app.services.baserow.requests.post", fake_post)

    client = HttpBaserowClient(
        base_url="http://localhost:8080",
        token="database-token",
        table_id="603",
        admin_email="stxtofr.local@example.com",
        admin_password="password",
    )

    client.ensure_review_schema()

    assert headers_seen[0]["Authorization"] == "JWT jwt-token"
    assert headers_seen[1]["Authorization"] == "JWT jwt-token"
