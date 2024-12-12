# app/tests/test_endpoints.py
from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

def test_total_licenses():
    response = client.post("/dto/lm/medico", json={
        "fecha_inicio": "2024-01-01",
        "fecha_fin": "2024-12-31",
        "folio": "12345"
    })
    assert response.status_code == 200
    assert "total_licencias" in response.json()
