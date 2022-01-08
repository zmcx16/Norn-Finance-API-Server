from fastapi.testclient import TestClient

from main import app

client = TestClient(app)


def test_hello_norn():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"msg": "Hello Norn"}


def test_ws_hello_norn():
    with client.websocket_connect("/ws") as websocket:
        data = websocket.receive_json()
        assert data == {"msg": "Hello Norn"}
