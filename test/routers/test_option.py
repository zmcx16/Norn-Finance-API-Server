from fastapi.testclient import TestClient

from main import app

client = TestClient(app)


def test_options_chain_quotes():
    response = client.get("/option/quote?symbols=T,AAPL")
    assert response.status_code == 200
    print(response.json())
