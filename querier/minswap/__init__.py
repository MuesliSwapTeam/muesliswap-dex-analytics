import logging

from cardano_python_utils.classes import Datum, Token, Asset, TxO  # type: ignore

from ..datum import parse_int, parse_bytes, OrderValues

_LOGGER = logging.getLogger(__name__)

"""
Minswap datum:
  // propertyList - list holding the different things required
  // 1. Trader Information (pubKeyHash + stakingCredentialHash) (sender according to contract)
  // 2. Trader Information (pubKeyHash + stakingCredentialHash) (receiver according to contract)
  // 3. Optional Receiver Datum Hash (what does this do? maybe ensure that output datum has this value, constructor 1 is prob None)
  // 4. Want token info / Swap step
  // 5. Laminar Batcher Fee
  // 6. Deposit
  See https://github.com/OpenSwap-Technologies-AG/muesliswap-frontend/blob/dev/src/cardano/aggregator/minswap.js
  
"""


def parse_token(datum: Datum) -> Token:
    assert datum.keys() == set(("fields", "constructor")), "invalid nested datum keys"
    fields = datum["fields"]
    assert isinstance(fields, list) and len(fields) == 2, "expected 2 fields for token"
    pid, tname = fields
    return Token(
        policy_id=parse_bytes(pid, "policy-id"), name=parse_bytes(tname, "tokenname")
    )


def parse_swap_exact_asset(datum: Datum) -> Asset:
    assert datum.keys() == set(("fields", "constructor")), "invalid nested datum keys"
    if datum["constructor"] in (3, 4):
        raise AssertionError("Zap-in order: not supported")  # basically they'd get the LP tokens instead
    assert datum["constructor"] in (0, 1), f"invalid swap asset constructor: {datum['constructor']}"
    fields = datum["fields"]
    assert isinstance(fields, list) and len(fields) == 2, "expected 2 fields for asset"
    token, amount = fields
    return Asset(amount=parse_int(amount, "swap amount"), token=parse_token(token))


def parse_sender_pkh(datum: Datum, key: str):
    try:
        sender_pkh_dict = datum["fields"][0]
        sender_skh_dict = datum["fields"][1]
        sender_pkh = sender_pkh_dict["fields"][0]["bytes"]
        sender_skh = sender_skh_dict["fields"][0]["fields"][0]["fields"][0]["bytes"]  # :-//
    except:
        raise AssertionError(f"Invalid minswap datum for {key}")
    return sender_pkh, sender_skh


def determine_bid_asset_from_txo(txo: dict, subtract_lvl: int) -> Asset:
    # If we want to parse the placed asset in general:
    # A) bid token is ADA: take txo.amount, subtract lvl_attached and batcher_fee
    #    - in some DEXs we don't have lvl_attached, but directly specified buy_amount
    # B) bid token is something else: here, fees are lvl_attached maybe plus batcher fee => don't check rn
    attached_assets = txo["value"].get("assets", {})
    attached_lvl = txo["value"]["coins"]

    if len(attached_assets) == 1:  # The easy case, single token placed
        # We are swapping to ADA
        # Note: Here we could drop TXs with incorrect lvl attached, if that's required for some DEX
        token, amount = list(attached_assets.items())[0]
        return Asset(token=Token.from_hex(token), amount=amount)
    elif len(attached_assets) == 0:
        # We are swapping from ADA
        amount = attached_lvl
        if subtract_lvl > 0:
            amount -= subtract_lvl
            assert amount > 0, "Invalid order: zero or negative bid amount after subtracting fees"
        return Asset(amount=amount, token=Token("", ""))
    else:
        assert False, f"Found {len(attached_assets)} attached assets in order: not implemented"


def parse_swap_datum(txo: dict, datum: Datum) -> OrderValues:
    assert datum, "expected a datum"
    assert datum.keys() == {"fields", "constructor"}, "invalid nested datum keys"
    fields = datum["fields"]
    assert isinstance(fields, list) and len(fields) == 6, "expected 6 fields for swap"
    sender, beneficiary, _, asset, batcher_fee, lvl_attached = fields

    sender_pkh, sender_skh = parse_sender_pkh(sender, "sender")
    beneficiary_pkh, beneficiary_skh = parse_sender_pkh(beneficiary, "beneficiary")

    batcher_fee = parse_int(batcher_fee, "batcher fee")
    lvl_attached = parse_int(lvl_attached, "lvl attached")

    return OrderValues(
        sender=(sender_pkh, sender_skh),
        beneficiary=(beneficiary_pkh, beneficiary_skh),
        bid_asset=determine_bid_asset_from_txo(txo, subtract_lvl=batcher_fee + lvl_attached), # unknown
        ask_asset=parse_swap_exact_asset(asset),
        batcher_fee=batcher_fee,
        lvl_deposit=lvl_attached
    )


def parse_redeemer(redeemer):
    if redeemer["constructor"] == 0:
        return "fulfilled"
    else:
        return "canceled"