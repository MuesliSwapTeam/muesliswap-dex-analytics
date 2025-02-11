import json
import logging
from datetime import datetime
from typing import Optional

import sqlalchemy as sqla
from cardano_python_utils import datums
from cardano_python_utils import kupo as kupo  # type: ignore
from cardano_python_utils.classes import (  # type: ignore
    Token,
    TxHash,
    Bech32Addr,
    TxO,
    Asset,
    Token,
    Datum,
)
from cardano_python_utils.kupo import KupoDatumBackend  # type: ignore
from cardano_python_utils.kupo import KupoDatumBackend  # type: ignore
from cardano_python_utils.util import ShelleyAddress
from sqlalchemy import orm, exc

from querier import minswap, wingriders, sundaeswap, vyfi
from . import util, db
from .config import DEFAULT_START_SLOT, DEFAULT_START_HASH
from .datum import OrderValues, OrderStatus
from .ogmios import OgmiosIterator

_LOGGER = logging.getLogger(__name__)
ADA = Token("", "")
MAX_INT_64 = 9223372036854775807


class VolumeCounter:
    slot_window: int
    engine: sqla.Engine
    max_slot_no: int

    def __init__(
        self,
        ogmios: OgmiosIterator,
        update_pool_addrs,
        echo: bool = True,
        start_slot_no: int = DEFAULT_START_SLOT,
        start_block_hash: str = DEFAULT_START_HASH,
    ) -> None:

        # address -> dex_id
        self.ogmios = ogmios
        self.update_pool_addrs = update_pool_addrs
        self.addr_to_dex_map, self.next_pools_update = self.update_pool_addrs()
        # dex_id -> dex
        self.dex_to_db_map = {
            dex_id: db.get_dex(dex_id)
            for dex_id, addr_type in set(self.addr_to_dex_map.values())
        }
        self.start_slot_no = start_slot_no
        self.start_block_hash = start_block_hash
        self.engine = db.create_engine(echo=echo)
        self.open_orders = self.initialize_open_orders()

    def initialize_open_orders(self) -> set:
        _LOGGER.info("Initializing set of open orders")
        open_orders = set()
        with orm.Session(self.engine) as session:
            orders = session.query(db.Order).filter_by(status="open")
            for order in orders:
                tx = order.tx
                open_orders.add(f"{tx.hash}#{tx.output_idx}")
        _LOGGER.info("Set of open orders initialized")
        return open_orders

    def parse_swap_datum(
        self, tx, txo, dex_id: str, tx_type: str, datum: Datum
    ) -> OrderValues:
        if dex_id == "minswap":  # TODO: pass the function in constructor
            return minswap.parse_swap_datum(txo, datum)
        elif dex_id == "sundaeswap":
            return sundaeswap.parse_swap_datum(txo, datum)
        elif dex_id == "wingriders":
            return wingriders.parse_swap_datum(txo, datum)
        elif dex_id == "vyfi":
            return vyfi.parse_swap_datum(txo, datum)
        else:
            raise AssertionError(f"Not implemented: {dex_id}")

    def run(self):
        # TODO: save max. block after each tx, or process by block
        for block in self.ogmios.iterate_blocks(
            self.start_slot_no, self.start_block_hash
        ):
            if datetime.now() >= self.next_pools_update:
                _LOGGER.info("Updating pool addresses")
                self.addr_to_dex_map, self.next_pools_update = self.update_pool_addrs()

            with orm.Session(self.engine) as session:
                try:
                    self.process_block(block, session)
                    session.commit()
                except exc.IntegrityError as err:
                    _LOGGER.error(
                        "Insertion of new elements failed!! Caught the following error: \n",
                        err,
                    )
                    session.rollback()
            # Get the newest list of liquidity pools periodically

    def process_block(self, block, session):
        block = block["result"]["block"]
        _LOGGER.info(f"Current block height: {block['height']}")
        if "transactions" not in block or block["transactions"] == []:
            _LOGGER.info("Skipping block without transactions")
            return
        for tx_idx, tx in enumerate(block["transactions"]):
            for input_idx, input in enumerate(tx["inputs"]):
                input_hash = f"{input['transaction']['id']}#{input['index']}"
                if input_hash in self.open_orders:
                    self.open_orders.remove(input_hash)
                    self.process_match_tx(
                        block, tx, input["transaction"]["id"], input["index"], session
                    )
            for output_idx, output in enumerate(tx["outputs"]):
                if output["address"] in self.addr_to_dex_map:
                    self.process_order_tx_output(
                        tx, tx_idx, output, output_idx, block, session
                    )

    def process_order_tx_output(self, tx, tx_idx, output, output_idx, block, session):
        _LOGGER.info(f"parsing TxO {tx['id']}#{tx_idx}")

        # Tx inputs are array of {txid, index} that we have to keep track of
        address = output["address"]
        dex_id, addr_type = self.addr_to_dex_map[address]
        try:
            datum_hex = output.get("datum")
            datum_hash = output.get("datumHash")
            assert (datum_hex is not None) or (
                datum_hash is not None
            ), "Datum(Hash) was not attached"
            if datum_hex is not None:
                datum = datums.datum_from_cborhex(datum_hex)
            else:
                assert (
                    "datums" in tx and datum_hash in tx["datums"]
                ), "Datum for this datum_hash was not attached"
                datum_hex = tx["datums"][datum_hash]
                datum = datums.datum_from_cborhex(datum_hex)
            swap_data = self.parse_swap_datum(tx, output, dex_id, addr_type, datum)
            validate_order(swap_data)
            _LOGGER.info(
                f"{dex_id}: Sell {swap_data.bid_asset} for {swap_data.ask_asset}"
            )
            self.open_orders.add(f"{tx['id']}#{output_idx}")
            self.add_order_to_db(
                session, dex_id, block, tx_idx, tx, output_idx, output, swap_data
            )
        except AssertionError:
            _LOGGER.exception("Error while parsing order")

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
    ):

        bid_token = db.get_token(swap_data.bid_asset.token, session)
        ask_token = db.get_token(swap_data.ask_asset.token, session)

        aggregator_platform, is_aggregator_order = None, False
        if tx.get("metadata"):
            md = tx["metadata"]["labels"]
            aggregator_platform = match_metadata(md)
            is_aggregator_order = aggregator_platform is not None

        dex = self.dex_to_db_map[dex_id]

        tx_id, tx_inputs, tx_outputs = tx["id"], tx["inputs"], tx["outputs"]

        tx = db.Tx(
            hash=tx_id,
            output_idx=output_idx,
            slot_no=block["slot"],
            header_hash=block["id"],
            index=tx_idx,
            owner=output["address"],
            timestamp=util.slot_timestamp(block["slot"]),
        )

        order = db.Order(
            dex=dex,
            tx=tx,
            ask_token_id=ask_token,
            bid_token_id=bid_token,
            ask_token=ask_token,
            bid_token=bid_token,
            ask_amount=swap_data.ask_asset.amount,
            bid_amount=swap_data.bid_asset.amount,
            batcher_fee=swap_data.batcher_fee,
            lvl_deposit=swap_data.lvl_deposit,
            is_muesliswap=is_aggregator_order,
            aggregator_platform=aggregator_platform,
            sender_pkh=swap_data.sender[0],
            sender_skh=swap_data.sender[1],
            beneficiary_pkh=swap_data.beneficiary[0],
            beneficiary_skh=swap_data.beneficiary[1],
            status=OrderStatus.OPEN,
        )

        session.add(dex)
        session.add(tx)
        session.add(order)

    def process_match_tx(
        self, block: dict, tx: dict, order_tx_hash: str, output_idx: int, session
    ) -> bool:
        tx_ = (
            session.query(db.Tx)
            .filter_by(hash=order_tx_hash, output_idx=output_idx)
            .first()
        )
        if tx_ is None:
            _LOGGER.error("Can't process match because order is not in DB (???)")
            return False
        order = tx_.order
        ask_token = order.ask_token
        ask_token = Token(ask_token.policy_id, ask_token.name)
        bid_token = order.bid_token
        bid_token = Token(bid_token.policy_id, bid_token.name)

        sender_addr = ShelleyAddress(
            pubkeyhash=order.sender_pkh, stakekeyhash=order.sender_skh, mainnet=True
        )

        beneficiary_addr = ShelleyAddress(
            pubkeyhash=order.beneficiary_pkh,
            stakekeyhash=order.beneficiary_skh,
            mainnet=True,
        )

        _LOGGER.info(f"Processing match tx {tx['id']}")

        # Check if the order was fulfilled or canceled
        for output_idx, output in enumerate(tx["outputs"]):
            # print(order.beneficiary, output["address"])
            if output["address"] == beneficiary_addr.bech32:
                amount_fulfilled = check_fulfilled_amount(
                    ask_token, bid_token, output, order.lvl_deposit
                )
                if amount_fulfilled > 0:
                    _LOGGER.info(
                        f"Order fulfilled {amount_fulfilled / order.ask_amount * 100 :.2f} %"
                    )
                    order.status = (
                        OrderStatus.FULFILLED
                    )  # TODO: do we need a partially fulfilled status?
                    order.fulfilled_amount = amount_fulfilled
                    order.finalized_at = util.slot_timestamp(block["slot"])
                    return True
            if output["address"] == sender_addr.bech32:
                amount_canceled = check_fulfilled_amount(
                    bid_token, ask_token, output, order.lvl_deposit
                )
                if amount_canceled > 0:
                    _LOGGER.info(
                        f"Order canceled {amount_canceled / order.bid_amount * 100 :.2f} %"
                    )
                    order.status = OrderStatus.CANCELLED
                    order.finalized_at = util.slot_timestamp(block["slot"])
                    return True

        # TODO: Alternatively if no match is detected and tx signed by sender, it's cancellation (not observed so far)
        _LOGGER.warning(f"Order spent in unknown way: {tx['id']}")
        order.status = OrderStatus.UNKNOWN
        return False


def match_metadata(metadata: dict) -> Optional[str]:
    metadata_str = json.dumps(metadata).lower()
    if "674" in metadata and "muesli" in metadata_str or "eternl" in metadata_str:
        return "muesli" if "muesli" in metadata_str else "eternl"
    return None


def check_fulfilled_amount(
    ask_token: Token, bid_token: Token, txo: dict, lvl_deposit: int
):
    # TODO: this didn't work for tx 4b09500c7f08cbaddcceeb2a38c1743b3cef4f1c7b3e31f19adaefbc513d4f0c
    # TODO: also for 7a1dec377e71b9511f372122abef2a8f2d5a4d5d45bd8e81942537773d9cd145 and other tx
    if ask_token == ADA and bid_token.to_hex() not in util.parse_assets(txo):
        amount_fulfilled = txo["value"]["ada"]["lovelace"] - lvl_deposit
    else:
        amount_fulfilled = util.parse_assets(txo).get(ask_token.to_hex(), 0)
    return amount_fulfilled


def validate_order(order_values: OrderValues):
    assert (
        order_values.ask_asset.amount > 0
    ), f"Ask amount is {order_values.ask_asset.amount}"
    assert (
        order_values.bid_asset.amount > 0
    ), f"Bid amount is {order_values.bid_asset.amount}"
    assert order_values.batcher_fee >= 0, f"Batcher fee is {order_values.batcher_fee}"
    assert order_values.lvl_deposit > 0, f"Lvl attached is {order_values.lvl_deposit}"
    # TODO: for some DEXs, make sure that placed asset in datum matches actually attached asset
    assert (
        order_values.ask_asset.token != order_values.bid_asset.token
    ), "Ask and bid tokens are the same!"
    assert order_values.ask_asset.amount <= MAX_INT_64, "Ask amount: 64-bit overflow"
    assert order_values.bid_asset.amount <= MAX_INT_64, "Bid amount: 64-bit overflow"
