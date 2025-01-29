import json
import logging
import os
import time
from decimal import Decimal
from cardano_python_utils.util import atomic_dump

from . import util
from . import DATA_DIR, CACHE_DEFAULT_TIMEOUT

_LOGGER = logging.getLogger(__name__)
_LOGGER.setLevel(logging.INFO)


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


def generate_data():
    # Generate data
    all_token_stats = util.get_all_token_stats()
    atomic_dump(json.dumps(all_token_stats, cls=DecimalEncoder), DATA_DIR / "all_token_stats.json")
    generate_token_pair_stats(all_token_stats)


def generate_token_pair_stats(all_token_stats: list):
    _LOGGER.info(f"Saving token pair stats ...")
    os.makedirs(DATA_DIR / "token_pair_stats", exist_ok=True)

    for token_stats in all_token_stats:
        from_token, to_token = token_stats["fromToken"], token_stats["toToken"]
        filename = f"{from_token}-{to_token}.json"
        atomic_dump(json.dumps(token_stats, cls=DecimalEncoder), DATA_DIR / "token_pair_stats" / filename)

    _LOGGER.info(f"Token pair stats saved")


if __name__ == '__main__':
    while True:
        start_time = time.time()
        generate_data()
        end_time = time.time()
        _LOGGER.info(f"Generated all data in {end_time - start_time:.2f} seconds")
        # Sleep at most max cache time minutes (to reduce server stress, can jank this up if needed)
        time.sleep(max(0, CACHE_DEFAULT_TIMEOUT - int(end_time - start_time)))
