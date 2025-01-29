"""
Benchmark for the server/util.py module
Run with pytest --durations=0
"""
import datetime

from server.util import *

def test_all_stats_multi(subtests):
    now = int(datetime.datetime(year=2023, month=8, day=29).timestamp())
    cache = {}
    with subtests.test(i=-1):
        get_all_token_stats(time_to=now, cache=cache)

    for i in range(5):
        with subtests.test(i=i):
            get_all_token_stats(time_to=now+i, cache=cache)

def test_current_volume_no_filters():
    now = datetime.datetime(year=2023, month=8, day=29)
    get_current_volume(time_from=int(now.timestamp()), time_to=int((now + datetime.timedelta(days=1)).timestamp()))

def test_history_many():
    history = get_order_history(
        sender_pkh="5247dd3bdf2d2f838a2f0c91b38f127523772d24393993e10fbbd235",
        sender_skh="9a45a01d85c481827325eca0537957bf0480ec37e9ada731b06400d0",
        aggregator_id=None,
        limit=100000
    )

def test_history_few():
    history = get_order_history(
        sender_pkh="ffff25d841c6b21970c6c5339bd4bc7827bb90e609a6744299b7939b",
        sender_skh="6dabc2efba65d7e853dd0d25e19188ad0dc7a30ee924010212a2e2be",
        aggregator_id=None,
        limit=100000
    )
