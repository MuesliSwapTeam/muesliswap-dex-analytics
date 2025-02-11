import dataclasses
import logging
from datetime import datetime

import requests  # type: ignore
from cardano_python_utils.classes import (  # type: ignore
    TxHash,
    Bech32Addr,
    TxIndex,
    TxO,
    Asset,
    Token, Datum, CBorHex,
)
from cardano_python_utils.kupo import KupoApi  # type: ignore
from pycardano import PlutusData

_LOGGER = logging.getLogger(__name__)


class InvalidOrderException(AssertionError):
    """Must be called unconditionally BEFORE modifying any objects in sqlalchemy!"""
    pass


def slot_timestamp(slot_no: int) -> int:
    return 1596491091 + (slot_no - 4924800)


def slot_datestring(slot_no: int) -> int:
    return datetime.utcfromtimestamp(slot_timestamp(slot_no)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )

@dataclasses.dataclass
class MuesliSwapV1OrderParams(PlutusData):
    CONSTR_ID = 0
    creator_pkh: bytes
    buy_token_policy: bytes
    buy_token_name: bytes
    buy_amount: int

@dataclasses.dataclass
class MuesliSwapV1Datum(PlutusData):
    CONSTR_ID = 0
    order_params: MuesliSwapV1OrderParams

def reconstruct_datum_metadata(tx: dict) -> CBorHex:
    """
    Example metadata (tx['metadata']):
    {
    'hash': 'aef2c13dcce9006bb0f4a4e6f0a1e353b403894cb451009d8e9a3af8db18c7d6',
    'body': {'blob': {
    '1000': {'bytes': '01353b8bc29a15603f0b73eac44653d1bd944d92e0e0dcd5eb185164a2da22c532206a75a628778eebaf63826f9d93fbe9b4ac69a7f8e4cd78'},
    '1001': {'string': '353b8bc29a15603f0b73eac44653d1bd944d92e0e0dcd5eb185164a2'},
    '1002': {'string': ''},
    '1003': {'string': ''},
    '1004': {'string': '10000000'}
    }, 'scripts': []}}
    """
    try:
        metadata = tx["metadata"]["body"]["blob"]
        creator = metadata["1001"]["string"]
        buy_token = metadata["1002"]["string"], metadata["1003"]["string"].encode("utf8").hex()
        buy_amount = metadata["1004"].get("int", metadata["1004"].get("string"))
        return CBorHex(MuesliSwapV1Datum(
            MuesliSwapV1OrderParams(
                bytes.fromhex(creator),
                bytes.fromhex(buy_token[0]),
                bytes.fromhex(buy_token[1]),
                int(buy_amount)
            )
        ).to_cbor_hex())
    except (KeyError, ValueError, AssertionError, TypeError) as e:
        _LOGGER.warning(f"Failed to reconstruct metadata from tx {tx['id']}: {e}")
        return None

ADA = Token("", "")


def parse_assets(txo: dict):
    assets_without_ada = {k: v for k, v in txo["value"].items() if k != "ada"}
    return {
        f"{outer_key}.{inner_key}": value
        for outer_key, inner_dict in assets_without_ada.items()
        for inner_key, value in inner_dict.items()
    }