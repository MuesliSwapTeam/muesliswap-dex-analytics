from dataclasses import dataclass
from typing import Tuple
from fractions import Fraction

from cardano_python_utils.classes import Datum, Asset, Bech32Addr  # type: ignore


@dataclass
class OrderValues:
    sender: Tuple[Bech32Addr, Bech32Addr]
    beneficiary: Tuple[Bech32Addr, Bech32Addr]
    bid_asset: Asset
    ask_asset: Asset
    batcher_fee: int
    lvl_deposit: int


class OrderStatus:
    OPEN = "open"
    CANCELLED = "canceled"
    FULFILLED = "matched"
    UNKNOWN = "unknown"


def parse_int(datum: Datum, desc: str = "") -> int:
    assert datum.keys() == set(["int"]), f"expected int as {desc}"
    val = datum["int"]
    assert isinstance(val, int), f"expected int as {desc}, got {val!r}"
    return val


def parse_bytes(datum: Datum, desc: str = "") -> str:
    assert datum.keys() == set(["bytes"]), f"expected bytes as {desc}"
    val = datum["bytes"]
    assert isinstance(val, str), f"expected bytes as {desc}, got {val!r}"
    return val


def parse_str(datum: Datum, desc: str = "") -> str:
    assert datum.keys() == set(["string"]), f"expected str as {desc}"
    val = datum["string"]
    assert isinstance(val, str), f"expected string as {desc}, got {val!r}"
    return val


def parse_frac(datum: Datum, desc: str = "") -> Fraction:
    num, denom = datum["fields"][0]["int"], datum["fields"][1]["int"]
    assert isinstance(num, int) and isinstance(
        denom, int
    ), f"expected (int, int) as {desc}, got {num!r} and {denom!r}"
    return Fraction(num, denom)
