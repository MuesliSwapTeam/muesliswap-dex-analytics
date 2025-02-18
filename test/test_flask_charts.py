import time
import pytest
from server.serve import app

client = app.test_client()


def test_price_chart():
    t = int(time.time())
    base_token = ""
    quote_token = "8a1cfae21368b8bebbbed9800fec304e95cce39a2a57dc35e2e3ebaa4d494c4b"
    from_timestamp = t - 86400
    to_timestamp = t
    endpoint_url = (
        f"/charts/price/?base-token={base_token}&quote-token={quote_token}"
        f"&from-timestamp={from_timestamp}&to-timestamp={to_timestamp}"
    )
    response = client.get(endpoint_url)
    assert response.status_code == 200
    response = client.get(endpoint_url + "&dex-id=muesli")
    assert response.status_code == 200
    response = client.get(endpoint_url + "&aggregator-id=muesli")
    assert response.status_code == 200
    response = client.get(endpoint_url + "&dex-id=muesli&aggregator-id=muesli")
    assert response.status_code == 200
