import time
import pytest
from server.serve import app

client = app.test_client()


def test_orderbook():
    t = int(time.time())
    base_token = ""
    quote_token = "8a1cfae21368b8bebbbed9800fec304e95cce39a2a57dc35e2e3ebaa4d494c4b"
    # from ada
    endpoint_url = f'/trades/?from-token={base_token}&to-token={quote_token}'
    response = client.get(endpoint_url)
    assert response.status_code == 200
    # to ada
    endpoint_url = f'/trades/?from-token={quote_token}&to-token={base_token}'
    response = client.get(endpoint_url)
    assert response.status_code == 200
    # twice the same token
    endpoint_url = f'/trades/?from-token={quote_token}&to-token={quote_token}'
    response = client.get(endpoint_url)
    assert response.status_code == 400
    # only one token
    endpoint_url = f'/trades/?from-token={quote_token}'
    response = client.get(endpoint_url)
    assert response.status_code == 400


def test_trades():
    base_token = ""
    quote_token = "8a1cfae21368b8bebbbed9800fec304e95cce39a2a57dc35e2e3ebaa4d494c4b"
    endpoint_url = (f'/trades/?from-token={base_token}&to-token={quote_token}')
    # all orders
    response = client.get(endpoint_url)
    assert response.status_code == 200
    # only aggregator orders
    response = client.get(endpoint_url + "&aggregator-id=muesli")
    assert response.status_code == 200
