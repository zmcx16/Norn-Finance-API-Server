from fastapi.testclient import TestClient

from main import app

client = TestClient(app)


def test_stock_history():
    response = client.get("/stock/history?symbol=T")
    assert response.status_code == 200
    print(response.json())
