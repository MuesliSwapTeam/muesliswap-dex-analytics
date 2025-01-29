import logging

from cardano_python_utils import datums
from cardano_python_utils.classes import Token, Asset, ShelleyAddress

from querier.datum import OrderValues
from querier.util import ADA, InvalidOrderException, MuesliSwapV1Datum

_LOGGER = logging.getLogger(__name__)

def parse_swap_datum(tx: dict, txo: dict, datum: datums.Datum) -> OrderValues:
    try:
        datum = MuesliSwapV1Datum.from_dict(datum)
    except Exception as e:
        _LOGGER.error(f"Failed to parse MuesliSwapV1Datum: {e}")
        raise InvalidOrderException() from e

    attached_lovelace = None
    full_address = None
    if tx.get("metadata") is not None:
        metadata = tx["metadata"]["body"]["blob"]
        try:
            full_address = ShelleyAddress.from_hex(metadata["1000"]["bytes"].replace("0x", ""))
        except Exception:
            pass
        try:
            attached_lovelace = int(metadata["1005"]["int"])
        except KeyError:
            pass
    if attached_lovelace is None:
        attached_lovelace = 1900000
    if full_address is None:
        full_address = ShelleyAddress(pubkeyhash=datum.order_params.creator_pkh.hex(), mainnet=True)
        for txo in tx["body"]["outputs"]:
            if ShelleyAddress.from_bech32(txo["address"]).pubkeyhash == full_address.pubkeyhash:
                full_address = ShelleyAddress.from_bech32(txo["address"])

    if datum.order_params.buy_token_policy != b"":
        sell_token = ADA
        sell_asset = Asset(txo["value"]["coins"] - attached_lovelace, sell_token)
    else:
        sell_token = None
        sell_asset = None
        for asset, value in txo["value"]["assets"].items():
            sell_token = Token.from_hex(asset)
            sell_asset = Asset(value, sell_token)
            break
        assert sell_token is not None, "No sell token found"

    sender_shelley = full_address
    sender_pkh, sender_skh = sender_shelley.pubkeyhash, sender_shelley.stakekeyhash
    buy_asset = Asset(datum.order_params.buy_amount, Token(datum.order_params.buy_token_policy.hex(), datum.order_params.buy_token_name.hex()))

    batcher_fee = 650000
    return OrderValues(
        sender=(sender_pkh, sender_skh),
        beneficiary=(sender_pkh, sender_skh),
        bid_asset=sell_asset,
        ask_asset=buy_asset,
        batcher_fee=batcher_fee,
        lvl_deposit=attached_lovelace - batcher_fee,
    )


def parse_redeemer(redeemer):
    if redeemer["constructor"] == 0:
        return "canceled"
    else:
        return "fulfilled"