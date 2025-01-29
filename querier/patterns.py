import logging
from datetime import datetime, timedelta

from querier.config import SUNDAE_ORDER_CONTRACT, \
    MINSWAP_ORDER_CONTRACT, \
    MINSWAP_ORDER_CONTRACT_NOSTAKE, \
    WINGRIDERS_ORDER_CONTRACT, MINSWAP_V0_ORDER_CONTRACT, \
    SPLASH_ORDER_CONTRACT
from querier.vyfi import get_vyfi_pools_list

_LOGGER = logging.getLogger(__name__)


MUESLISWAP_V1_ORDERBOOK = "addr1wy2mjh76em44qurn5x73nzqrxua7ataasftql0u2h6g88lc3gtgpz"
MUESLISWAP_V2_ORDERBOOK = "addr1z8c7eyxnxgy80qs5ehrl4yy93tzkyqjnmx0cfsgrxkfge27q47h8tv3jp07j8yneaxj7qc63zyzqhl933xsglcsgtqcqxzc2je"
MUESLISWAP_V3_ORDERBOOK = "addr1z8l28a6jsx4870ulrfygqvqqdnkdjc5sa8f70ys6dvgvjqc3r6dxnzml343sx8jweqn4vn3fz2kj8kgu9czghx0jrsyqxyrhvq"
MUESLISWAP_V4_ORDERBOOK = "addr1zyq0kyrml023kwjk8zr86d5gaxrt5w8lxnah8r6m6s4jp4g3r6dxnzml343sx8jweqn4vn3fz2kj8kgu9czghx0jrsyqqktyhv"

def get_all_pool_addrs():
    """
    This function fetches any dynamic pool addresses, updates patterns in kupo,
    and returns an up-to-date map from pool addresses to dex ids.
    """
    addr_to_dex_map = {
        SUNDAE_ORDER_CONTRACT: ('sundaeswap', 'order'),
        MINSWAP_ORDER_CONTRACT: ('minswap', 'order'),
        MINSWAP_ORDER_CONTRACT_NOSTAKE: ('minswap', 'order'),
        MINSWAP_V0_ORDER_CONTRACT: ('minswap', 'order'),
        WINGRIDERS_ORDER_CONTRACT: ('wingriders', 'order'),
        SPLASH_ORDER_CONTRACT: ('splash', 'order'),
        MUESLISWAP_V1_ORDERBOOK: ('muesliswap_v1', 'order'),
        MUESLISWAP_V2_ORDERBOOK: ('muesliswap', 'order'),
        MUESLISWAP_V3_ORDERBOOK: ('muesliswap', 'order'),
        MUESLISWAP_V4_ORDERBOOK: ('muesliswap', 'order'),
    }

    vyfi_pools = get_vyfi_pools_list()  # download, cache and add to kupo new vyfi batcher addresses
    for pool in vyfi_pools.keys():
        addr_to_dex_map[pool] = ('vyfi', 'order')

    return addr_to_dex_map


def update_patterns():
    pool_addr_map = get_all_pool_addrs()
    next_pools_update = datetime.now() + timedelta(hours=1)
    return pool_addr_map, next_pools_update
