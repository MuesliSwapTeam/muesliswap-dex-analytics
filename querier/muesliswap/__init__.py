import dataclasses
import enum
import orjson
import logging
from typing import List, Tuple, Union, Optional

import sqlalchemy

from querier import db, util, patterns, OrderStatus
from cardano_python_utils import datums
from cardano_python_utils.classes import Token, Asset, CBorHex
from cardano_python_utils.util import ShelleyAddress

from querier.util import InvalidOrderException, ADA

_LOGGER = logging.getLogger(__name__)
ORDER_CONTRACTS = [
    patterns.MUESLISWAP_V1_ORDERBOOK,
    patterns.MUESLISWAP_V2_ORDERBOOK,
    patterns.MUESLISWAP_V3_ORDERBOOK,
    patterns.MUESLISWAP_V4_ORDERBOOK
]


class RedeemerConstructor(enum.IntEnum):
    CANCEL = 0
    FULL_MATCH = 1
    PARTIAL_MATCH = 2


@dataclasses.dataclass(frozen=True)
class BlockInfo:
    slot_no: int
    header_hash: str
    timestamp: int
    tx_index: int
    tx_hash: str


def get_place_tokens_and_amounts():
    raise NotImplementedError()

def find_datum_cbor_hex(tx: dict, txo: dict) -> CBorHex | None:
    datum_hash = txo.get("datumHash")
    if not datum_hash:
        _LOGGER.warning("Datum hash was not attached")
        return None

    if tx.get("datum") is not None:
        return tx["datum"]
    datum_hex = tx["witness"]["datums"].get(datum_hash)
    return datum_hex

def parse_wallet_address(datum: datums.Datum):
    pkh = datum["fields"][0]["fields"][0]["bytes"]
    skh_cons = datum["fields"][1]
    try:
        skh = skh_cons["fields"][0]["fields"][0]["fields"][0]["bytes"]
    except (KeyError, IndexError):
        assert skh_cons["constructor"] == 1
        skh = ""
    return (pkh, skh)


def parse_muesliswap_datum(datum: datums.Datum):
    try:
        datum = datum["fields"][0]["fields"]
        creator = parse_wallet_address(datum[0])
        buy_token = Token(datum[1]["bytes"], datum[2]["bytes"])
        sell_token = Token(datum[3]["bytes"], datum[4]["bytes"])
        buy_amount = int(datum[5]["int"])
        allow_partial = datum[6]["constructor"] == 1
        # not actually used by SC, frontend only; someone even omits this
        lovelace_attached = int(datum[7]["int"]) if len(datum) > 7 else 0
        return (creator, buy_token, sell_token, buy_amount, allow_partial, lovelace_attached)
    except (KeyError, AssertionError, IndexError, NotImplementedError) as ex:
        _LOGGER.exception("Can't parse muesliswap datum")
        raise InvalidOrderException(ex)


def reconstruct_datum_metadata(tx: dict, txo: dict) -> Tuple[Tuple[str, str], Token, Token, int, bool, int] | None:
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
        full_address = metadata["1000"]["bytes"].replace("0x", "")[2:]
        creator = full_address[:56], full_address[56:]
        buy_token = Token(metadata["1002"]["string"], metadata["1003"]["string"].encode("utf8").hex())
        buy_amount = int(metadata["1004"]["string"])
        try:
            attached_lovelace = int(metadata["1005"]["int"])
        except KeyError:
            attached_lovelace = 1900000
        allow_partial = False
        if buy_token != ADA:
            sell_token = ADA
        else:
            sell_token = None
            for asset in txo["value"]["assets"]:
                sell_token = Token.from_hex(asset)
                break
            assert sell_token is not None, "No sell token found"
        return creator, buy_token, sell_token, buy_amount, allow_partial, attached_lovelace
    except (KeyError, ValueError, AssertionError, TypeError) as e:
        _LOGGER.warning(f"Failed to reconstruct metadata from tx {tx['id']}: {e}")
        return None


def determine_sell_amount(txo: dict, sell_token: Token, lvl_attached: int) -> int:
    attached_assets = txo["value"].get("assets", {})
    txo_lvl = txo["value"]["coins"]

    if sell_token.policy_id == "" and sell_token.name == "":
        # Selling ADA, lvl attached is only used for display and not relevant for SC
        amount = txo_lvl - lvl_attached
        # this can happen if it is a partial match from a block before we started indexing (or sender messed up datum)
        # this can ALSO happen if the matchmaker ignores lvl_attached and uses up a part of it
        if amount < 0:
            _LOGGER.warning(f"Invalid MuesliSwap datum: lvl_attached ({lvl_attached}) is higher than actual amount ({txo_lvl})")
        return amount

    else:
        # Selling some other token -> find it
        amount = attached_assets.get(sell_token.to_hex(), 0)
        if amount <= 0:
            _LOGGER.warning("Invalid MuesliSwap order: specified sell token wasn't attached")
        # this can happen if it is a partial match from a block before we started indexing
        if txo_lvl != lvl_attached:
            _LOGGER.warning(f"Unexpected MuesliSwap attached lvl {txo_lvl} != datum lvl_attached field ({lvl_attached})")
        return amount


def create_utxo_for_order(block_info: BlockInfo, txo_index: int, txo_owner: str, txo: dict, datum_cbor_hex: CBorHex):
    return db.Tx(
        id=f"{block_info.tx_hash}#{txo_index}",
        hash=block_info.tx_hash,
        output_idx=txo_index,
        slot_no=block_info.slot_no,
        header_hash=block_info.header_hash,
        index=block_info.tx_index,
        owner=txo_owner,
        timestamp=block_info.timestamp,
        value=orjson.dumps(txo["value"]),
        datum_hash=txo.get("datumHash"),
        inline_datum=txo.get("datum", txo.get("datumHash")) != txo.get("datumHash"),
        datum=datum_cbor_hex,
    )


def process_place_order(
        block_info: BlockInfo,
        tx: dict,
        output_ix: int,
        session: db.Session,
        partial_matches: List[db.PartialMatch]
) -> Tuple[Optional[db.Order], Optional[db.Tx]]:

    txo = tx["body"]["outputs"][output_ix]
    # Get amounts from datum
    datum_hex = find_datum_cbor_hex(tx, txo)
    if datum_hex is None:
        _LOGGER.warning(f"Could not find attached datum for hex {datum_hex}")
        datum = None
    else:
        datum = datums.datum_from_cborhex(datum_hex)
    if datum is None:
        _LOGGER.warning(f"Skipping muesliswap txo {block_info.tx_hash}#{output_ix} with invalid datum")
        return None, None
    creator, buy_token, sell_token, buy_amount, allow_partial, datum_lvl_attached = parse_muesliswap_datum(datum)
    if buy_amount <= 0:
        _LOGGER.info(f"Ask amount of txo {output_ix} is {buy_amount}")
        buy_amount = 0
    if datum_lvl_attached <= 0:
        _LOGGER.info(f"Datum lovelace attached of txo {output_ix} is {buy_amount}")
        datum_lvl_attached = 0

    # depending on context this is either the partial match utxo or the original placed utxo
    db_tx = create_utxo_for_order(block_info, output_ix, txo["address"], txo, datum_hex)

    # If this order is the result of a partial match, we don't create a new Order object
    for pm in partial_matches:
        if (pm.order.beneficiary_pkh == creator[0] and
                pm.order.beneficiary_skh == creator[1] and
                pm.order.ask_token == buy_token.subject and
                pm.order.tx.owner == txo["address"]
        ):  # or status = partially matched?
            ask_token_amount = get_amount_of(txo, Token.from_hex(pm.order.ask_token))
            pm.new_utxo = db_tx
            # matched_amount and paid_amount are deltas, i.e.:
            # in this partial match, the user received (amount in TxOut - amount in TxIn) of ask_token
            pm.matched_amount = ask_token_amount - pm.order.fulfilled_amount
            # in this partial match, the user provided (amount in TxIn - amount in TxOut) of bid_token
            # note: if paid_amount > bid_amount, numbers will be negative, but the result is correct
            bid_amount_leftover = determine_sell_amount(txo, sell_token, datum_lvl_attached)
            pm.paid_amount = (pm.order.bid_amount - pm.order.paid_amount) - bid_amount_leftover

            # after the partial match, the order now contains these amounts of ask_ and bid_token
            pm.order.fulfilled_amount = ask_token_amount
            pm.order.paid_amount = pm.order.bid_amount - bid_amount_leftover

            partial_matches.remove(pm)
            session.add(pm)
            _LOGGER.info(f"Partial match new UTXO {db_tx.hash}#{db_tx.output_idx}")
            return pm.order, db_tx

    # important: whatever amount of "ask_token" was provided COUNTS towards fulfilled_amount
    provided_amount = get_amount_of(txo, buy_token)
    return db.Order(
        dex_id="muesliswap",
        tx=db_tx,
        ask_amount=buy_amount,  # if selling token->ada, lvl_deposit DOES count towards this
        bid_amount=determine_sell_amount(txo, sell_token, datum_lvl_attached),
        fulfilled_amount=provided_amount,  # if this is ADA, lvl_deposit IS included in this
        paid_amount=0,  # this can go higher than bid_amount iff matchmaker ignores lvl_attached datum
        ask_token=buy_token.subject,
        bid_token=sell_token.subject,
        batcher_fee=0,  # we can't know this ahead of time with the current smart contract
        # Expected batcher fee may be < 0, but it must hold that bid_amt + fee + lvl == amount sent in tx
        lvl_deposit=datum_lvl_attached,
        sender_pkh=creator[0],
        sender_skh=creator[1],
        # This is always the same as sender
        beneficiary_pkh=creator[0],
        beneficiary_skh=creator[1],
        status=OrderStatus.OPEN,
        is_muesliswap=True,
    ), db_tx


def process_cancel_order(txi_hash: str, txi_idx: int, block_info: BlockInfo, session: db.Session):
    placed_order = get_placed_order(txi_hash, txi_idx, session)
    if placed_order is None:
        return None  # happens if we don't start syncing from origin
    cancellation = db.Cancellation(order=placed_order, slot_no=block_info.slot_no, tx_hash=block_info.tx_hash)
    session.add(cancellation)
    _LOGGER.info(f"Cancel order {txi_hash}#{txi_idx}")
    return cancellation


def get_amount_of(utxo: dict, token: Token) -> int:
    if token.subject == "":
        return utxo['value']['coins']
    return utxo['value']['assets'].get(token.to_hex(), 0)


def get_placed_order(utxo_hash: str, utxo_index: str | int, session: db.Session) -> Optional[db.Order]:
    utxo_id = f"{utxo_hash}#{utxo_index}"
    # It can be either the original placed order ...
    placed_order = session.query(db.Order).where(db.Order.tx_id == utxo_id).first()
    if placed_order:
        return placed_order
    # ... or a PartialMatch
    partial_match = session.query(db.PartialMatch).where(db.PartialMatch.new_utxo_id == utxo_id).first()
    if partial_match:
        return partial_match.order
    # This can't happen right now since we keep an "open_orders" set like now,
    # but in general could happen if we don't start syncing from origin
    _LOGGER.error(f"Placed order not found in database for UTXO: {utxo_id}")
    return None


def process_full_match(tx: dict, txo_hash: str, txo_idx: int, block_info: BlockInfo, session: db.Session):
    # 1. Get original placed order from this TXO
    placed_order = get_placed_order(txo_hash, txo_idx, session)
    if placed_order is None:
        return None
    # 2. Calculate how much ask_token the sender got
    sender_address = ShelleyAddress(pubkeyhash=placed_order.sender_pkh, stakekeyhash=placed_order.sender_skh, mainnet=True).bech32
    ask_token = Token.from_hex(placed_order.ask_token)
    ask_amount_rcvd = 0
    bid_amount_returned = 0
    for ix, output in enumerate(tx["body"]["outputs"]):
        # NOTE: in theory it has to be in one utxo with both ask_token and returned amount
        if output["address"] == sender_address and get_amount_of(output, ask_token) > 0:
            ask_amount_rcvd = get_amount_of(output, ask_token)
            # this is excluding lvl_deposit, can also result in a negative amount (not a problem)
            bid_amount_returned = determine_sell_amount(output, Token.from_hex(placed_order.bid_token), placed_order.lvl_deposit)
            break

    # 3. Update original order with the correct amount
    assert ask_amount_rcvd >= placed_order.ask_amount, (
        f"Expected at least {placed_order.ask_amount} {placed_order.ask_token} tokens in full match, but found only {ask_amount_rcvd}! {placed_order.tx_id}"
    )
    # see partialmatch for explanation of these amounts
    full_match = db.FullMatch(
        order=placed_order,
        matched_amount=ask_amount_rcvd - placed_order.fulfilled_amount,  # delta TxOut - TxIn
        paid_amount=(placed_order.bid_amount - placed_order.paid_amount) - bid_amount_returned,  # delta TxIn - TxOut
        slot_no=block_info.slot_no,
        tx_hash=block_info.tx_hash
    )
    placed_order.fulfilled_amount = ask_amount_rcvd
    placed_order.paid_amount = (placed_order.bid_amount - placed_order.paid_amount) - bid_amount_returned

    session.add(full_match)
    _LOGGER.info(f"Full match order {txo_hash}#{txo_idx} with amount {ask_amount_rcvd}")
    return full_match


def process_partial_match(txo_hash: str, txo_idx: int, block_info: BlockInfo, session: db.Session):
    # 1. Get original placed order from this TXO
    placed_order = get_placed_order(txo_hash, txo_idx, session)
    if placed_order is None:
        return None  # happens if we don't start syncing from origin
    # placed_order.status = OrderStatus.OPEN
    _LOGGER.info(f"Partial match order {txo_hash}#{txo_idx}")
    # Here, PartialMatch is only returned and the new order UTXO is linked to it later when processing outputs
    return db.PartialMatch(
        order=placed_order, slot_no=block_info.slot_no
    )


def process_input_txo(
        tx: dict, input_ix: int, in_txo: dict, block_info: BlockInfo, session: db.Session
) -> Union[db.PartialMatch, db.FullMatch, db.Cancellation, None]:
    try:
        redeemer_cbor = tx["witness"]["redeemers"][f"spend:{input_ix}"]["redeemer"]
        redeemer = datums.datum_from_cborhex(redeemer_cbor)
        redeemer_action = RedeemerConstructor(redeemer["constructor"])
    except (KeyError, ValueError):
        _LOGGER.exception(f"Invalid redeemer for input TxO {input_ix}")
        return None

    txo_hash, txo_idx = in_txo["txId"], in_txo["index"]

    if redeemer_action == RedeemerConstructor.CANCEL:
        return process_cancel_order(txo_hash, txo_idx, block_info, session)
    elif redeemer_action == RedeemerConstructor.FULL_MATCH:
        return process_full_match(tx, txo_hash, txo_idx, block_info, session)
    elif redeemer_action == RedeemerConstructor.PARTIAL_MATCH:
        return process_partial_match(txo_hash, txo_idx, block_info, session)


def process_muesliswap_tx(block: dict, tx_idx: int, session: db.Session, open_orders: set):
    """
    :param block: Block object from Ogmios
    :param tx_idx: Position of tx in block
    :param output_idx: Index of TxO that should be processed
    :param session: Database session
    :param open_orders: Set of txhash#index open orders found in this TX
    """
    tx = block["body"][tx_idx]
    block_slot, block_hash = block["header"]["slot"], block["headerHash"]
    timestamp = util.slot_timestamp(block_slot)
    # Tx inputs/outputs that are sitting at the orderbook
    contract_ins = [(ix, i) for ix, i in enumerate(tx["body"]["inputs"]) if f"{i['txId']}#{i['index']}" in open_orders]
    contract_outs = [(ix, o) for ix, o in enumerate(tx["body"]["outputs"]) if o["address"] in ORDER_CONTRACTS]

    _LOGGER.info(f"Parsing MuesliSwap TxO {tx['id']}")
    # First, determine type of TxO (place, cancel, etc.)
    partial_matches = []
    block_info = BlockInfo(slot_no=block_slot, header_hash=block_hash, timestamp=timestamp,
                           tx_index=tx_idx, tx_hash=tx["id"])
    if len(contract_ins) > 0:
        # Cancel or Match
        for ix, txo in contract_ins:
            obj = process_input_txo(tx, ix, txo, block_info, session)
            if isinstance(obj, db.PartialMatch):
                partial_matches.append(obj)
            elif obj is None:
                txo_hash, txo_idx = txo["txId"], txo["index"]
                _LOGGER.warning(f"Muesliswap open order left unprocessed!!! This will cause problems with orderbook! UTXO: {txo_hash}#{txo_idx}")
    new_utxos = []
    if len(contract_outs) > 0:
        # Place order
        for ix, txo in contract_outs:
            # it can now happen that one PlaceOrder in the TX is processed and the second throws
            # then only the valid subset of this TX will be processed
            try:
                order, new_utxo = process_place_order(block_info, tx, ix, session, partial_matches)
                if order is not None:
                    session.add(order)  # related objects with "cascade" in relationship will also be added
                    new_utxos.append(new_utxo)
            except InvalidOrderException:
                _LOGGER.exception("Skipped invalid order - transaction will be incomplete!")

    # Partial matches are cleared from list by process_place_order
    assert not partial_matches, f"Found {len(partial_matches)} unpaired partial matches!"
    return new_utxos
