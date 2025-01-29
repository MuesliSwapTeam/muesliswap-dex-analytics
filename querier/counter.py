import re
from collections import defaultdict

import ahocorasick
import orjson
import logging
from time import perf_counter
from datetime import datetime
from typing import Optional

import pycardano
import sqlalchemy
import sqlalchemy as sqla
from cardano_python_utils import datums
from cardano_python_utils import kupo as kupo  # type: ignore
from cardano_python_utils.classes import (  # type: ignore
    Token,
    TxHash,
    Bech32Addr,
    TxO,
    Asset,
    Datum
)
from cardano_python_utils.kupo import KupoDatumBackend  # type: ignore
from cardano_python_utils.kupo import KupoDatumBackend  # type: ignore
from cardano_python_utils.util import ShelleyAddress
from sqlalchemy import orm

from querier import minswap, wingriders, sundaeswap, vyfi, OrderStatus, BlockIterator, muesliswap_v1
from . import util, db
from .datum import OrderValues
from .muesliswap import process_muesliswap_tx
from .util import ADA, reconstruct_datum_metadata, InvalidOrderException

_LOGGER = logging.getLogger(__name__)
MAX_INT_64 = 9223372036854775807
DISABLE_RAPID_DISCARD_SLOT = 49310409


class VolumeCounter:
    slot_window: int
    engine: sqla.Engine
    max_slot_no: int

    def __init__(self,
                 ogmios: BlockIterator,
                 update_pool_addrs,
                 echo: bool = True,
                 ) -> None:

        # address -> dex_id
        self.ogmios = ogmios
        self.update_pool_addrs = update_pool_addrs
        self.addr_to_dex_map, self.next_pools_update = self.update_pool_addrs()
        self.engine = db._ENGINE
        self.open_orders = self.initialize_open_orders()  # this has to stay consistent!
        # update after fetching pools
        self._rapid_discard_filter_update = True
        self.current_slot = -1

    @property
    def rapid_discard_filter(self):
        # this needs to be updated whenever the contracts or the open orders change!
        if self._rapid_discard_filter_update:
            self._cached_rapid_discard_filter = self.generate_rapid_discard_filter()
            self._tx_hash_count = defaultdict(int)
            self._rapid_discard_filter_update = False
        if self._cached_rapid_discard_filter.kind != ahocorasick.AHOCORASICK:
            self._cached_rapid_discard_filter.make_automaton()
        return self._cached_rapid_discard_filter

    def add_open_order(self, tx_id: str, dex: str):
        self.open_orders[tx_id] = dex
        if self.current_slot < DISABLE_RAPID_DISCARD_SLOT:
            tx_hash = tx_id[:64]
            self._tx_hash_count[tx_hash] += 1
            self._cached_rapid_discard_filter.add_word(tx_hash, True)

    def remove_open_order(self, tx_id: str):
        del self.open_orders[tx_id]
        if self.current_slot < DISABLE_RAPID_DISCARD_SLOT:
            tx_hash = tx_id[:64]
            prev_count = self._tx_hash_count[tx_hash]
            self._tx_hash_count[tx_hash] = prev_count - 1
            if prev_count == 1:
                self._cached_rapid_discard_filter.remove_word(tx_hash)


    def generate_rapid_discard_filter(self):
        # either an open order is spent or a transaction sends to one of the tracked contracts
        automaton = ahocorasick.Automaton()
        for key in self.open_orders.keys():
            automaton.add_word(key[:64], True)
        for contract in self.addr_to_dex_map.keys():
            automaton.add_word(contract, True)
        automaton.make_automaton()
        return automaton

    def initialize_open_orders(self) -> dict:
        _LOGGER.info("Initializing set of open orders")
        t = perf_counter()
        open_orders = dict()
        query = """
        with last_partial_match as (
            select order_id, max(slot_no) as slot_no
            from "PartialMatch"
            group by order_id
        )
        SELECT
        COALESCE(pm_tx.hash, tx.hash),
        COALESCE(pm_tx.output_idx, tx.output_idx),
        o.dex_id
        FROM "Order" o
        JOIN "Tx" tx ON o.tx_id = tx.id
        LEFT OUTER JOIN last_partial_match lpm ON o.id = lpm.order_id
        LEFT OUTER JOIN "PartialMatch" pm ON lpm.slot_no = pm.slot_no
        LEFT OUTER JOIN "Tx" pm_tx ON pm.new_utxo_id = pm_tx.id
        WHERE o.full_match_id IS NULL AND o.cancellation_id IS NULL
        """
        with orm.Session(self.engine) as session:
            sql_query = sqlalchemy.text(query)
            result = session.execute(sql_query)
            for row in result:
                open_orders[f"{row[0]}#{row[1]}"] = row[2]
        t = perf_counter() - t
        _LOGGER.info(f"Set of open orders initialized in {t} seconds")
        return open_orders

    def parse_swap_datum(self, tx, txo, dex_id: str, tx_type: str, datum: Datum) -> OrderValues:
        if dex_id == 'minswap':
            return minswap.parse_swap_datum(txo, datum)
        elif dex_id == 'sundaeswap':
            return sundaeswap.parse_swap_datum(txo, datum)
        elif dex_id == 'wingriders':
            return wingriders.parse_swap_datum(txo, datum)
        elif dex_id == 'vyfi':
            return vyfi.parse_swap_datum(txo, datum)
        elif dex_id == 'muesliswap_v1':
            return muesliswap_v1.parse_swap_datum(tx, txo, datum)
        else:
            raise AssertionError(f"Not implemented: {dex_id}")

    def run(self):
        self._last_discard_time = perf_counter()
        self._last_discard_count = 0
        for block in self.ogmios.iterate_blocks():
            if datetime.now() >= self.next_pools_update:
                # Get the newest list of liquidity pools periodically
                _LOGGER.info("Updating pool addresses")
                self.addr_to_dex_map, self.next_pools_update = self.update_pool_addrs()
                self._rapid_discard_filter_update = True
            with orm.Session(self.engine) as session:
                self.process_block(block, session)
                session.commit()

    def process_block(self, raw_block, session):
        # do some rapid pre-filtering to discard irrelevant blocks
        # NOTE: this only makes sense until slot 49310409 (rough estimate)
        #      after that, we should just process every block because the amount of open orders is large
        #      and there is a transaction in almost every block
        if self.current_slot < DISABLE_RAPID_DISCARD_SLOT:
            can_discard = next(self.rapid_discard_filter.iter(raw_block), None) is None
            if can_discard:
                self._last_discard_count += 1
                return
            time = perf_counter() - self._last_discard_time
            _LOGGER.info(f"%s Blocks discarded in %.2f seconds (%.4f)", self._last_discard_count, time, (time / (self._last_discard_count + 1)))

        # actually load the block properly
        resp = orjson.loads(raw_block)
        block = resp["result"]["RollForward"]["block"]

        block_keys = list(block.keys())
        # There should be exactly one key like "babbage" or "alonzo" etc.
        assert len(block_keys) == 1, "Encountered weird block from 2 eras"

        block = block[block_keys[0]]
        self.current_slot = block['header']['slot']
        block_time = datetime.fromtimestamp(util.slot_timestamp(self.current_slot))
        _LOGGER.info(f"Processing block height: {self.current_slot} ({block_time.isoformat()})")
        for tx_idx in range(len(block["body"])):
            try:
                self.process_transaction(block, tx_idx, session)
            except util.InvalidOrderException:
                _LOGGER.exception("Skipped invalid order")
        self._last_discard_time = perf_counter()
        self._last_discard_count = 0

    def process_transaction(self, block: dict, tx_idx: int, session: db.Session):
        tx = block["body"][tx_idx]
        is_muesliswap_tx = False
        muesli_orders = set()

        for input_idx, input in enumerate(tx["body"]["inputs"]):
            input_hash = f"{input['txId']}#{input['index']}"  # - these are the only two fields Ogmios provides for us
            if input_hash in self.open_orders:  # therefore we can't just check the utxo address, we need to track
                dex = self.open_orders[input_hash]
                if dex == 'muesliswap':
                    is_muesliswap_tx = True
                    muesli_orders.add(input_hash)
                else:
                    self.process_match_tx(block, tx, input["txId"], input["index"], input_idx, dex, session)
                self.remove_open_order(f"{input['txId']}#{input['index']}")

        for output_idx, output in enumerate(tx["body"]["outputs"]):
            if output["address"] in self.addr_to_dex_map:
                dex_id, addr_type = self.addr_to_dex_map[output["address"]]
                if dex_id == "muesliswap":
                    is_muesliswap_tx = True
                else:
                    self.process_order_tx_output(tx, tx_idx, output, output_idx, block, session)

        if is_muesliswap_tx:
            new_open_utxos = process_muesliswap_tx(block, tx_idx, session, muesli_orders)
            for utxo in new_open_utxos:
                self.add_open_order(f"{utxo.hash}#{utxo.output_idx}", "muesliswap")
            # raise AssertionError("OK!")

    def process_order_tx_output(self, tx, tx_idx, output, output_idx, block, session):
        _LOGGER.info(f"parsing TxO {tx['id']}#{tx_idx}")

        # Tx inputs are array of {txid, index} that we have to keep track of
        address = output["address"]
        dex_id, addr_type = self.addr_to_dex_map[address]
        try:
            datum_hash = output.get("datumHash")
            assert datum_hash, "Datum hash was not attached"
            inline_datum = False
            # datum has to be either inlined or attached in witness set or can be reconstructed from metadata
            if output.get("datum") is not None and output["datum"] != output["datumHash"]:
                datum_hex = output["datum"]
                inline_datum = True
            elif datum_hash in tx["witness"]["datums"]:
                datum_hex = tx["witness"]["datums"][datum_hash]
            elif dex_id == "muesliswap_v1" and tx.get("metadata"):
                datum_hex = reconstruct_datum_metadata(tx)
                assert datum_hex is not None, "Failed to reconstruct datum from metadata"
                if pycardano.datum_hash(pycardano.RawCBOR(bytes.fromhex(datum_hex))).payload.hex() != datum_hash:
                    _LOGGER.warning("Reconstructed datum hash does not match")
            else:
                raise AssertionError("No datum found")
            try:
                datum = datums.datum_from_cborhex(datum_hex)
            except Exception as e:
                raise AssertionError("Error while parsing datum") from e
            swap_data = self.parse_swap_datum(tx, output, dex_id, addr_type, datum)

            validate_order(swap_data)
            _LOGGER.info(f"{dex_id}: Sell {swap_data.bid_asset} for {swap_data.ask_asset}")
            self.add_open_order(f"{tx['id']}#{output_idx}", dex_id)
            self.add_order_to_db(session, dex_id, block, tx_idx, tx, output_idx, output, swap_data, datum_hash, datum_hex, inline_datum)
        except AssertionError as e:
            _LOGGER.exception("Error while parsing order: %s", e)
            raise InvalidOrderException()

    def add_order_to_db(
            self,
            session,
            dex_id: str,
            block: dict,
            tx_idx: int,
            tx: dict,
            output_idx: int,
            output,
            swap_data: OrderValues,
            datum_hash: str,
            datum_cbor_hex: str,
            inline_datum: bool
    ):

        aggregator_platform, is_aggregator_order = None, False
        if tx.get("metadata"):
            md = tx["metadata"]["body"]["blob"]
            aggregator_platform = match_metadata(md)
            is_aggregator_order = aggregator_platform is not None

        # Cleanup for some weird eternl things
        if aggregator_platform == "eternl":
            # I know that the dict is frozen... but let's just roll with it
            if swap_data.ask_asset.token.subject == "2222":
                object.__setattr__(swap_data.ask_asset, 'token', Token("", ""))
            elif swap_data.bid_asset.token.subject == "2222":
                object.__setattr__(swap_data.bid_asset, 'token', Token("", ""))

        tx_id, tx_inputs, tx_outputs = tx["id"], tx["body"]["inputs"], tx["body"]["outputs"]

        session.execute(sqla.insert(db.Tx).values({
            "id": f"{tx_id}#{output_idx}",
            "hash": tx_id, "output_idx": output_idx,
            "slot_no": block["header"]["slot"],
            "header_hash": block["headerHash"],
            "index": tx_idx,
            "owner": output["address"],
            "timestamp": util.slot_timestamp(block["header"]["slot"]),
            "value": orjson.dumps(output["value"]),
            "datum_hash": datum_hash,
            "inline_datum": inline_datum,
            "datum": datum_cbor_hex,
        }))
        session.execute(sqla.insert(db.Order).values(**{
            "dex_id": dex_id,
            "tx_id": f"{tx_id}#{output_idx}",
            "ask_token": swap_data.ask_asset.token.subject,
            "bid_token": swap_data.bid_asset.token.subject,
            "ask_amount": swap_data.ask_asset.amount,
            "bid_amount": swap_data.bid_asset.amount,
            "fulfilled_amount": 0,
            "paid_amount": 0,
            "batcher_fee": swap_data.batcher_fee,
            "lvl_deposit": swap_data.lvl_deposit,
            "sender_pkh": swap_data.sender[0],
            "sender_skh": swap_data.sender[1],
            "beneficiary_pkh": swap_data.beneficiary[0],
            "beneficiary_skh": swap_data.beneficiary[1],
            "status": OrderStatus.OPEN,
            "is_muesliswap": is_aggregator_order,
            "aggregator_platform": aggregator_platform,
        }))

    def parse_redeemer(self, tx, input_idx, dex_id: str) -> OrderValues:
        redeemer_cbor = tx["witness"]["redeemers"][f"spend:{input_idx}"]["redeemer"]
        try:
            redeemer = datums.datum_from_cborhex(redeemer_cbor)
        except Exception as e:
            raise AssertionError("Error while parsing redeemer") from e
        if dex_id == 'minswap':
            return minswap.parse_redeemer(redeemer)
        elif dex_id == 'sundaeswap':
            return sundaeswap.parse_redeemer(redeemer)
        elif dex_id == 'wingriders':
            return wingriders.parse_redeemer(redeemer)
        elif dex_id == 'vyfi':
            return vyfi.parse_redeemer(redeemer)
        elif dex_id == 'muesliswap_v1':
            return muesliswap_v1.parse_redeemer(redeemer)
        else:
            raise AssertionError(f"Not implemented: {dex_id}")

    def process_match_tx(self, block: dict, tx: dict, order_tx_hash: str, order_output_idx: int, spent_input_idx: int, dex: str, session) -> bool:
        tx_ = session.query(db.Tx).filter_by(id=f"{order_tx_hash}#{order_output_idx}").first()
        if tx_ is None:
            _LOGGER.error("Can't process match because order is not in DB (???)")
            return False
        order = tx_.order
        ask_token = Token.from_hex(order.ask_token)
        bid_token = Token.from_hex(order.bid_token)

        sender_addr = ShelleyAddress(
            pubkeyhash=order.sender_pkh,
            stakekeyhash=order.sender_skh,
            mainnet=True
        ).bech32

        beneficiary_addr = ShelleyAddress(
            pubkeyhash=order.beneficiary_pkh,
            stakekeyhash=order.beneficiary_skh,
            mainnet=True
        ).bech32

        _LOGGER.info(f"Processing match tx {tx['id']}")

        # Check if the order was fulfilled or canceled
        status = "unknown"
        amount_fulfilled, amount_canceled = 0, 0

        for output_idx, output in enumerate(tx["body"]["outputs"]):
            # ouff, minswap may send it in multiple utxo (one for lvl_deposit)!
            if output["address"] == beneficiary_addr:
                amount_fulfilled += get_token_amount(ask_token, output, order.lvl_deposit)
            if output["address"] == sender_addr:
                # NOTE: This does not take into account that the user might have also provided
                # the input utxo
                # See following example: https://cardanoscan.io/transaction/2d52a63f790b4d4b617bccba55da0ebab6f68a4b7114f02b1df6c7d2eb8ea276?tab=utxo
                # The BE reports that 10 WMT were returned during cancelation, but forgets to take into account that the user also supplied some input
                amount_canceled += get_token_amount(bid_token, output, int(order.lvl_deposit))

        status = self.parse_redeemer(tx, spent_input_idx, dex)

        if status == "fulfilled":
            _LOGGER.info(f"Order fulfilled {amount_fulfilled / int(order.ask_amount) * 100 :.2f} %")
            paid_amount = order.bid_amount - amount_canceled
            matched_amount = amount_fulfilled
            if dex == "wingriders":
                # Wingriders returns excessive attached batcher fee, which is therefore not paid
                if ask_token == ADA:
                    # if we ordered ADA then we get back part of the batcher fee which should not count as matched
                    # but it is impossible to figure out whether the batcher took the full amount or not
                    # without looking at the pool or the outputs of the batcher
                    pass
                else:
                    # if we ordered a token then we get back the 2 ada intended batcher fee minus the actual batcher fee,
                    # hence the intended amount is the actual paid amount
                    paid_amount = order.bid_amount
            session.add(db.FullMatch(order=order, matched_amount=matched_amount, paid_amount=paid_amount,
                                     slot_no=self.current_slot, tx_hash=tx["id"]))
            order.fulfilled_amount = matched_amount
            order.paid_amount = paid_amount
            return True

        elif status == "canceled":
            _LOGGER.info(f"Order canceled {amount_canceled / order.bid_amount * 100 :.2f} %")
            session.add(db.Cancellation(order=order, slot_no=self.current_slot, tx_hash=tx["id"]))
            return True

        # NOTE: this includes muesliswap v1 orders that are returned to the wrong address
        _LOGGER.warning(f"Order spent in unknown way: {tx['id']}")
        session.add(db.FullMatch(order=order, matched_amount=-1, paid_amount=-1,
                                 slot_no=self.current_slot, tx_hash=tx["id"]))
        order.fulfilled_amount = -1
        order.paid_amount = -1
        return False


def match_metadata(metadata: dict) -> Optional[str]:
    metadata_str = orjson.dumps(metadata).lower()
    if "674" in metadata and b"muesli" in metadata_str or b"eternl" in metadata_str:
        return "muesli" if b"muesli" in metadata_str else "eternl"
    return None

def to_ogmios_token_string(tk: Token) -> str:
    return f"{tk.policy_id}.{tk.name}" if tk.name else tk.policy_id


def get_token_amount(token: Token, txo: dict, lvl_deposit: int) -> int:
    if token == ADA:
        return max(txo["value"]["coins"] - lvl_deposit, 0)  # this could be lower than zero

    return txo["value"]["assets"].get(to_ogmios_token_string(token), 0)


def validate_order(order_values: OrderValues):
    assert order_values.ask_asset.amount > 0, f"Ask amount is {order_values.ask_asset.amount}"
    assert order_values.bid_asset.amount > 0, f"Bid amount is {order_values.bid_asset.amount}"
    assert order_values.batcher_fee >= 0, f"Batcher fee is {order_values.batcher_fee}"
    assert order_values.lvl_deposit > 0, f"Lvl attached is {order_values.lvl_deposit}"
    assert order_values.ask_asset.token != order_values.bid_asset.token, "Ask and bid tokens are the same!"
    assert order_values.ask_asset.amount <= MAX_INT_64, "Ask amount: 64-bit overflow"
    assert order_values.bid_asset.amount <= MAX_INT_64, "Bid amount: 64-bit overflow"
