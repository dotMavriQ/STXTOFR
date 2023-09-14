from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)


def test_health() -> None:
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_providers_endpoint() -> None:
    response = client.get("/providers")
    assert response.status_code == 200
    names = {row["provider"] for row in response.json()}
    assert "circlek" in names
    assert "trafikverket" in names

