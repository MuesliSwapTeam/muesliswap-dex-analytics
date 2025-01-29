import time
import pytest
from server.serve import app

client = app.test_client()


def test_current_volume():
    t = int(time.time() - 86400)
    endpoints = ['/daily-volume?', '/weekly-volume?', f'/current-volume?time-from={t}&']
    for e in endpoints:
        response = client.get(e)
        assert response.status_code == 200
        response = client.get(f"{e}aggregator-id=muesli")
        assert response.status_code == 200


def test_dex_volume():
    t = int(time.time() - 86400)
    subject = ""
    endpoints = [f"/dex-volume?time-from={t}&", "/dex-volume/daily?", "/dex-volume/weekly?"]
    for e in endpoints:
        response = client.get(e + "token-subject=" + subject)
        assert response.status_code == 200
