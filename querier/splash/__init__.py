import logging

from cardano_python_utils.classes import Datum, Token, Asset  # type: ignore

from ..datum import parse_int, parse_bytes, OrderValues, parse_frac

_LOGGER = logging.getLogger(__name__)

"""
Minswap datum:
  // 1.  Order type
  // 2.  Beacon used to track progress
  // 3.  Input asset: what we receive
  // 4.  Tradable amount of Lovelace
  // 5.  Assumed cost (in Lovelace) of one step of execution
  // 6.  Minimal marginal output allowed per execution step
  // 7.  Output asset: what we spend
  // 8.  Worst acceptable price (output / intput)
  // 9.  How much fee we pay to executor for whole swap
  // 10. Address where the output from the order must go
  // 11. PKH authorized to cancel order
  // 12. List of Executors permitted to execute this order
  See https://github.com/splashprotocol/splash-core/blob/main/validators/orders/limit_order.ak
"""


def parse_token(datum: Datum) -> Token:
    assert datum.keys() == set(("fields", "constructor")), "invalid nested datum keys"
    fields = datum["fields"]
    assert isinstance(fields, list) and len(fields) == 2, "expected 2 fields for token"
    pid, tname = fields
    return Token(
        policy_id=parse_bytes(pid, "policy-id"), name=parse_bytes(tname, "tokenname")
    )


def parse_sender_pkh(datum: Datum, key: str):
    try:
        sender_pkh_dict = datum["fields"][0]
        sender_skh_dict = datum["fields"][1]
        sender_pkh = sender_pkh_dict["fields"][0]["bytes"]
        sender_skh = sender_skh_dict["fields"][0]["fields"][0]["fields"][0]["bytes"]  # :-//
    except:
        raise AssertionError(f"Invalid minswap datum for {key}")
    return sender_pkh, sender_skh


def determine_bid_asset_from_txo(txo: dict, tradable_lvl: int) -> Asset:
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
        return Asset(amount=tradable_lvl, token=Token("", ""))
    else:
        assert False, f"Found {len(attached_assets)} attached assets in order: not implemented"


def parse_swap_datum(txo: dict, datum: Datum) -> OrderValues:
    assert datum, "expected a datum"
    assert datum.keys() == {"fields", "constructor"}, "invalid nested datum keys"
    fields = datum["fields"]
    assert isinstance(fields, list) and len(fields) == 12, "expected 12 fields for standard limit order"
    otype, _, bid_token, tradable_lvl, cost, _, ask_token, worst_price, fee, beneficiary, _, _ = fields

    assert otype["bytes"] == "00", "order type not implemented"
    beneficiary_pkh, beneficiary_skh = parse_sender_pkh(beneficiary, "beneficiary")

    bid_asset = determine_bid_asset_from_txo(txo, tradable_lvl=tradable_lvl)
    price = parse_frac(worst_price)
    ask_asset = Asset(
        token=parse_token(ask_token),
        amount=int(price * bid_asset.amount),
    )    

    attached_lvl = txo["value"]["coins"]
    batcher_fee = parse_int(fee, "batcher fee")

    return OrderValues(
        sender=(beneficiary_pkh, beneficiary_skh),
        beneficiary=(beneficiary_pkh, beneficiary_skh),
        bid_asset=bid_asset,
        ask_asset=ask_asset,
        batcher_fee=batcher_fee,
        lvl_deposit=attached_lvl - tradable_lvl - batcher_fee,
    )


def parse_redeemer(redeemer):
    if redeemer["constructor"] == 1:
        return "fulfilled"
    else:
        return "canceled"