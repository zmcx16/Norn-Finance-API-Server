from fastapi.testclient import TestClient

from main import app

client = TestClient(app)


def test_options_chain_quotes():
    response = client.get("/option/quote?symbol=INTC")
    assert response.status_code == 200
    print(response.json())


def test_options_chain_quotes_valuation():
    response = client.get("/option/quote-valuation?symbol=T&ewma_his_vol_lambda=0.94")
    assert response.status_code == 200
    print(response.json())


def test_ws_options_chain_quotes_valuation():
    with client.websocket_connect("/ws/option/quote-valuation?symbol=T") as websocket:
        output = websocket.receive_json()
        assert output is not None
        assert len(output["contracts"]) > 0
        print(output)
