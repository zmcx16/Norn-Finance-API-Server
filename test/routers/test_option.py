import time
import threading
import asyncio

from fastapi.testclient import TestClient

from main import app

client = TestClient(app)


def test_options_chain_quotes():
    response = client.get("/option/quote?symbol=INTC")
    assert response.status_code == 200
    print(response.json())


def test_options_chain_quotes_valuation():
    # "/option/quote-valuation?symbol=T&ewma_his_vol_lambda=0.94&only_otm=false&specific_contract=call_2022-04-08_24"
    response = client.get("/option/quote-valuation?symbol=ZIM&ewma_his_vol_lambda=0.94&only_otm=true&"
                          "calc_kelly_iv=true&iteration=100000")
    assert response.status_code == 200
    output = response.json()
    print(output)
    assert len(output["contracts"]) > 0
    assert len(output["contracts"][0]['calls']) > 0 or len(output["contracts"][0]['puts'])


def test_ws_options_chain_quotes_valuation():
    with client.websocket_connect("/ws/option/quote-valuation?symbol=T&with_heartbeat=false") as websocket:
        output = websocket.receive_json()
        assert output is not None
        assert len(output["contracts"]) > 0
        print(output)
