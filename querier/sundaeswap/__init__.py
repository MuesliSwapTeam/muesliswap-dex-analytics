import logging
from datetime import datetime, timedelta

import requests
from cardano_python_utils import kupo as kupo  # type: ignore
from cardano_python_utils.classes import Bech32Addr, TxHash  # type: ignore
from cardano_python_utils.classes import (  # type: ignore
    Datum, Token, Asset, TxO, LOVELACE
)

from ..config import MUESLISWAP_POOLS_API_URL
from ..datum import parse_int, parse_bytes, OrderValues

cached_pools = {}
last_pools_update = datetime.min


def get_pools_list():
    global cached_pools, last_pools_update
    if not cached_pools or last_pools_update < datetime.now() - timedelta(hours=1):
        logging.info("Updating cached sundaeswap pool IDs from API")
        try:
            r = requests.get(MUESLISWAP_POOLS_API_URL)
            for pool in r.json():
                if pool["provider"] != "sundaeswap":
                    continue
                pool_id = pool["poolId"][61:]
                token_a, token_b = pool["tokenA"]["address"], pool["tokenB"]["address"]
                token_a = Token(token_a["policyId"], token_a["name"])
                token_b = Token(token_b["policyId"], token_b["name"])
                pool_dict = {"token_a": token_a, "token_b": token_b}
                cached_pools[pool_id] = pool_dict
            last_pools_update = datetime.now()
        except requests.exceptions.RequestException as e:
            logging.exception("Exception occurred when updating sundaeswap pools list")
    return cached_pools


def determine_lvl_attached(txo: dict, sell_asset: Asset, scooper_fee: int):
    attached_lvl = txo["value"]["coins"]
    attached_lvl -= scooper_fee

    if sell_asset.token.policy_id == "" and sell_asset.token.name == "":
        # Selling ADA - subtract order amount
        attached_lvl -= sell_asset.amount

    assert attached_lvl > 0, "Invalid sundaeswap order: not enough ADA attached for fees"
    return attached_lvl


def parse_swap_datum(txo: dict, datum: Datum) -> OrderValues:
    # Note: this function is token-to-token-swap proofed already, i believe

    assert datum, "expected a datum"
    assert datum.keys() == {"fields", "constructor"}, "invalid nested datum keys"
    fields = datum["fields"]
    assert isinstance(fields, list) and len(fields) == 4, "expected a 4 item list for sundaeswap"

    pool_id_obj, trader_info, scooper_fee_obj, token_amounts = fields
    pools = get_pools_list()
    pool_id = parse_bytes(pool_id_obj, "sundaeswap pool id")
    assert pool_id in pools, f"sundaeswap pool_id ({pool_id}) in datum is invalid or unknown"

    assert "fields" in token_amounts and len(token_amounts["fields"]) == 3, "expected 3 fields in tokenamounts"
    assert "constructor" in token_amounts["fields"][0], "expected a constructor for swap_dir"
    swap_dir = token_amounts["fields"][0]["constructor"]
    # const direction = equalsAddress(selectedPool.tokenA.address, fromToken.address) ? '0' : '1'
    # So, (swap_dir == 0) iff (token A == sold token) and therefore (token B == rcvd token)

    assert "fields" in token_amounts["fields"][2], "expected buy amount to have fields"
    buy_amount_fields = token_amounts["fields"][2]["fields"]
    assert len(buy_amount_fields) == 1, f"expected to find exactly one buy amount but found {len(buy_amount_fields)}"
    buy_amount = parse_int(buy_amount_fields[0], "buy amount")

    sell_amount = parse_int(token_amounts["fields"][1], "sell amount")

    pool = pools[pool_id]
    token_buy, token_sell = (pool["token_b"], pool["token_a"]) if swap_dir == 0 else (pool["token_a"], pool["token_b"])
    buy_asset = Asset(amount=buy_amount, token=token_buy)
    sell_asset = Asset(amount=sell_amount, token=token_sell)

    # if we swap from ada, then deposit is lvl attached minus lvl used for swap minus scooper fee
    # lvl_attached = (txo.amount - sell_amount - scooper_fee) if token_sell == LOVELACE else txo.amount (- scooper_fee?)

    sender_pkh, sender_skh = parse_sender_pkh(trader_info, "sender")
    scooper_fee = parse_int(scooper_fee_obj, "scooper fee")

    return OrderValues(
        sender=(sender_pkh, sender_skh),
        beneficiary=(sender_pkh, sender_skh),
        bid_asset=sell_asset,
        ask_asset=buy_asset,
        batcher_fee=scooper_fee,
        lvl_deposit=determine_lvl_attached(txo, sell_asset, scooper_fee)
    )


def parse_sender_pkh(datum: Datum, key: str):
    try:
        sender_pkh_dict = datum["fields"][0]["fields"][0]["fields"][0]["fields"][0]  # truly amazing schema design
        sender_pkh = sender_pkh_dict["bytes"]
        # 2x [0], then [1], then 2x [0]
        sender_skh_dict = datum["fields"][0]["fields"][0]["fields"][1]["fields"][0]["fields"][0]
        sender_skh = sender_skh_dict["fields"][0]["bytes"]
    except:
        raise AssertionError(f"Invalid sundaeswap datum for {key} (i don't blame them)")
    return sender_pkh, sender_skh


#
TEST_DATUM = {
    'constructor': 0,
    'fields': [
        {'bytes': '1f04'},   # 1. sundaeswap poolID
        {'constructor': 0,   # 2. trader information
         'fields': [  # 1x [0]
             {'constructor': 0,
              'fields': [  # 2x [0]
                 {
                    'constructor': 0,
                    'fields': [  # 3x [0]
                        {'constructor': 0,
                         'fields': [  # 4x [0]
                             {'bytes': '002572c40b64a0625367dedef78201885761dde304237dc68e4897c7'}
                         ]
                         },
                        {'constructor': 0,
                         'fields': [  # 2x [0], then [1], then [0]
                             {'constructor': 0,
                              'fields': [  # 2x [0], then [1], then 2x [0]
                                  {'constructor': 0,
                                   'fields': [  # YES NEST IT EVEN DEEPER IDIOTS FFS
                                       {'bytes': '6a75c76b0aceaada5cd01511ed986afdb4d8ea405db9ed43361c17ab'}
                                   ]
                                  }
                              ]
                             }
                         ]
                         }
                    ]
                 },
                 {'constructor': 1, 'fields': []}
             ]},
             {'constructor': 1, 'fields': []}
         ]},
        {'int': 2500000},   # 3. scooper fee
        {'constructor': 0,  # 4. token amounts
         'fields': [
             {'constructor': 0, 'fields': []},  # this is the swap direction
             {'int': 200000000},                # this is the sell amount
             {'constructor': 0, 'fields': [{'int': 201934}]}  # this is the buy amount
         ]}
    ]
}


def parse_redeemer(redeemer):
    if redeemer["constructor"] == 0:
        return "fulfilled"
    else:
        return "canceled"