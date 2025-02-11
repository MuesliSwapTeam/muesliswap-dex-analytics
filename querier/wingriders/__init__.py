import logging

from cardano_python_utils import kupo as kupo  # type: ignore
from cardano_python_utils.classes import Bech32Addr, TxHash, TxO  # type: ignore
from cardano_python_utils.classes import Datum, Token, Asset  # type: ignore

from ..datum import parse_int, parse_bytes, OrderValues
from ..util import parse_assets


WINGRIDERS_BATCHER_FEE = 2_000_000
WINGRIDERS_DEPOSIT_LVL = 2_000_000
_LOGGER = logging.getLogger(__name__)


def parse_sender_pkh(datum: Datum, key: str):
    try:
        sender_pkh_dict = datum["fields"][0]
        sender_skh_dict = datum["fields"][1]["fields"][0]["fields"][0]["fields"][0]
        sender_pkh = sender_pkh_dict["fields"][0]["bytes"]
        sender_skh = sender_skh_dict["bytes"]
    except:
        raise AssertionError(f"Invalid wingriders datum for {key}")
    return sender_pkh, sender_skh


def determine_sell_amount(txo: dict, sell_token: Token):
    attached_assets = parse_assets(txo)
    attached_lvl = txo["value"]["coins"]

    if sell_token.policy_id == "" and sell_token.name == "":
        # Selling ADA, determine from lvl attached
        amount = attached_lvl - WINGRIDERS_DEPOSIT_LVL - WINGRIDERS_BATCHER_FEE
        assert (
            amount > 0
        ), "Invalid wingriders order: not enough lvl attached after fees"
        return Asset(token=sell_token, amount=amount)

    else:
        # Selling some other token -> find it
        amount = attached_assets.get(sell_token.to_hex(), 0)
        assert (
            amount > 0
        ), "Invalid wingriders order: specified sell token wasn't attached"
        if attached_lvl != 4_000_000:
            _LOGGER.warning(
                f"Unexpected Wingriders attached lvl {attached_lvl} != 4_000_000"
            )
        return Asset(token=sell_token, amount=amount)


def parse_swap_datum(txo: dict, datum: Datum) -> OrderValues:
    assert datum, "expected a datum"
    assert datum.keys() == set(["fields", "constructor"]), "invalid nested datum keys"
    fields = datum["fields"]
    assert (
        isinstance(fields, list) and len(fields) == 2
    ), "expected 2 fields for wingriders"

    properties1, properties2 = fields
    assert (
        len(properties1["fields"]) == 4
    ), "properties 1 length must be 4 but is %d" % len(properties1["fields"])
    trader_obj, beneficiary_pkh, _, _ = properties1["fields"]
    tokens = get_assets(properties1)

    assert (
        len(properties2["fields"]) == 2
    ), "properties 2 length must be 2 but is %d" % len(properties2["fields"])
    swap_dir, buy_amount = properties2["fields"]
    assert "constructor" in swap_dir, "swap direction must have a constructor"
    swap_dir = swap_dir["constructor"]
    assert swap_dir in [0, 1], "Swap dir must be 0 or 1"
    buy_amount = parse_int(buy_amount, "buy amount")

    rcvd_policy_id, rcvd_token_name = tokens[1 - swap_dir]
    rcvd_token = Token(
        policy_id=parse_bytes(rcvd_policy_id, "policy-id"),
        name=parse_bytes(rcvd_token_name, "tokenname"),
    )
    rcvd_asset = Asset(amount=buy_amount, token=rcvd_token)

    place_policy_id, place_token_name = tokens[swap_dir]
    place_token = Token(
        policy_id=parse_bytes(place_policy_id, "policy-id"),
        name=parse_bytes(place_token_name, "tokenname"),
    )
    sender_pkh, sender_skh = parse_sender_pkh(trader_obj, "sender")

    return OrderValues(
        sender=(sender_pkh, sender_skh),
        beneficiary=(sender_pkh, sender_skh),
        bid_asset=determine_sell_amount(txo, place_token),
        ask_asset=rcvd_asset,
        batcher_fee=WINGRIDERS_BATCHER_FEE,
        lvl_deposit=WINGRIDERS_DEPOSIT_LVL,
    )


def get_assets(props1: Datum):
    fields = props1["fields"]
    assert (
        isinstance(fields, list) and len(fields) == 4
    ), "expected 4 fields for sundae swap #1"
    address_obj, pubkeyhash, unix_ts, token_constr = fields
    tokens_list = token_constr["fields"]
    assert (
        isinstance(tokens_list, list) and len(tokens_list) == 2
    ), "expected 2 tokens in sundae tx"
    token_a, token_b = tokens_list
    return [token_a["fields"], token_b["fields"]]


def parse_redeemer(redeemer):
    if redeemer["constructor"] == 0:
        return "fulfilled"
    else:
        return "canceled"
