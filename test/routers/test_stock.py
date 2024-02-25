from fastapi.testclient import TestClient

from main import app

client = TestClient(app)


def test_stock_history():
    response = client.get("/stock/history?symbol=T")
    assert response.status_code == 200
    print(response.json())


def test_price_simulation_by_mc():
    response = client.get("/stock/price-simulation-by-mc?symbol=T")
    assert response.status_code == 200
    print(response.json())


def test_stock_benford_law():
    response = client.get("/stock/benford-law?symbol=T")
    assert response.status_code == 200
    print(response.json())
