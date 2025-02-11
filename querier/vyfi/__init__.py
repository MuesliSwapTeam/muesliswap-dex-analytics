import orjson
import logging
from datetime import datetime, timedelta

import requests
from cardano_python_utils import kupo as kupo  # type: ignore
from cardano_python_utils.classes import Bech32Addr, TxHash  # type: ignore
from cardano_python_utils.classes import (  # type: ignore
    Datum, Token, Asset, TxO, LOVELACE
)

from ..config import VYFI_POOLS_ENDPOINT, VYFI_LVL_ATTACHED
from ..datum import parse_int, parse_bytes, OrderValues
from ..util import parse_assets

_LOGGER = logging.getLogger(__name__)

cached_pools = {}
last_pools_update = datetime.min


def get_vyfi_pools_list():
    global cached_pools, last_pools_update
    if not cached_pools or last_pools_update < datetime.now() - timedelta(hours=1):
        logging.info("Updating cached vyfi pools from API")
        try:
            r = requests.get(VYFI_POOLS_ENDPOINT)
            for pool in r.json():
                if not pool.get("isLive"):
                    continue
                token_a, token_b = pool["unitsPair"].split("/", maxsplit=1)
                pid_a, tname_a = ("", "") if token_a == 'lovelace' else (token_a[:56], token_a[56:])
                pid_b, tname_b = ("", "") if token_b == 'lovelace' else (token_b[:56], token_b[56:])
                token_a, token_b = Token(pid_a, tname_a), Token(pid_b, tname_b)
                metadata = orjson.loads(pool["json"])
                # is it poolValidatorUtxoAddress or orderValidatorUtxoAddress?
                order_address = pool["orderValidatorUtxoAddress"]  # order utxos sit here
                pool_address = pool["poolValidatorUtxoAddress"]  # pool utxo(s?) sit(s) here
                pool_dict = {
                    "token_a": token_a,
                    "token_b": token_b,
                    "batcher_fee": int(metadata["feesSettings"]["processFee"]),
                    "order_address": order_address,
                    "pool_address": pool_address
                }
                cached_pools[order_address] = pool_dict
            last_pools_update = datetime.now()
        except requests.exceptions.RequestException as e:
            logging.exception("Exception occurred when updating vyfi pools list")
    return cached_pools


def determine_sell_amount(txo: dict, sell_token: Token, batcher_fee: int):
    attached_assets = parse_assets(txo)
    attached_lvl = txo["value"]["coins"]

    if sell_token.policy_id == "" and sell_token.name == "":
        # Selling ADA, determine from lvl attached
        amount = attached_lvl - VYFI_LVL_ATTACHED - batcher_fee
        assert amount > 0, "Invalid VyFi order: not enough lvl attached after fees"
        return Asset(token=sell_token, amount=amount)

    else:
        # Selling some other token -> find it
        amount = attached_assets.get(sell_token.to_hex(), 0)
        assert amount > 0, "Invalid VyFi order: specified sell token wasn't attached"
        if attached_lvl != VYFI_LVL_ATTACHED + batcher_fee:
            _LOGGER.warning(f"Unexpected VyFi attached lvl {attached_lvl} != VYFI_LVL_ATTACHED + batcher_fee ({VYFI_LVL_ATTACHED} + {batcher_fee})")
        return Asset(token=sell_token, amount=amount)


def parse_swap_datum(txo: dict, datum: Datum) -> OrderValues:
    assert datum, "expected a datum"
    assert datum.keys() == {"fields", "constructor"}, "invalid nested datum keys"
    fields = datum["fields"]
    assert isinstance(fields, list) and len(fields) == 2, "expected a N item list for vyfi"

    trader_bytes, buy_object = fields
    trader = parse_bytes(trader_bytes, "trader bytes")
    trader_pkh, trader_skh = trader[:56], trader[56:]
    assert buy_object.keys() == {"fields", "constructor"}, "invalid nested datum keys"
    trade_direction, token_amounts_list = buy_object["constructor"], buy_object["fields"]

    # Direction 0 means pool.tokenA == from token
    # In constructor it's integer 3 (0) or 4 (1) ... if it's different, then it's some special order
    assert trade_direction in [3, 4], f"VyFi: unsupported trade direction: {trade_direction}"
    swap_dir = 0 if trade_direction == 3 else 1

    assert len(token_amounts_list), "VyFi: buy amount list must have at least one amount"
    buy_amount = parse_int(token_amounts_list[0], "buy amount")

    # buy and sell tokens are obtained by getting the metadata of the target liquidity pool
    assert txo["address"] in cached_pools, "utxo is not sitting at a known vyfi order address"
    pool = cached_pools[txo["address"]]

    token_buy, token_sell = (pool["token_b"], pool["token_a"]) if swap_dir == 0 else (pool["token_a"], pool["token_b"])
    recv_asset = Asset(amount=buy_amount, token=token_buy)
    place_asset = determine_sell_amount(txo, token_sell, pool["batcher_fee"])

    # if we swap from ada, then deposit is lvl attached minus lvl used for swap minus scooper fee
    # lvl_attached = (txo.amount - sell_amount - scooper_fee) if token_sell == LOVELACE else txo.amount (- scooper_fee?)

    return OrderValues(
        sender=(trader_pkh, trader_skh),
        beneficiary=(trader_pkh, trader_skh),
        bid_asset=place_asset,
        ask_asset=recv_asset,
        batcher_fee=pool["batcher_fee"],
        lvl_deposit=VYFI_LVL_ATTACHED,
    )


def parse_redeemer(redeemer):
    if redeemer["constructor"] == 0:
        return "fulfilled"
    else:
        return "canceled"