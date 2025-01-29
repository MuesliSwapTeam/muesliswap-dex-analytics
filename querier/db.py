import logging
import os
from typing import List

import sqlalchemy as sqla
from cardano_python_utils import classes  # type: ignore
from sqlalchemy import ForeignKey, Index
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
    Session,
)
from sqlalchemy.schema import UniqueConstraint
from sqlalchemy.engine import Engine
from sqlalchemy import event
from decimal import Decimal

from querier import OrderStatus, util


DATABASE_URI = os.environ.get("DATABASE_URI", "sqlite+pysqlite:///db.sqlite")
if DATABASE_URI.startswith("sqlite"):
    @event.listens_for(Engine, "connect")
    def set_sqlite_pragma(dbapi_connection, connection_record):
        """This makes sure that sqlite will respect cascade delete"""
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()


def create_engine(connect_string: str, echo: bool = False) -> sqla.Engine:
    return sqla.create_engine(connect_string, echo=echo, poolclass=sqla.pool.QueuePool)


_ENGINE = create_engine(DATABASE_URI, echo=False)


########################################################################################
#                                          DB Schema                                   #
########################################################################################


class Base(DeclarativeBase):
    pass

class Tx(Base):
    __tablename__ = "Tx"
    # NOTE this is not actually a Tx but a UTXO (single output of a transaction)

    id: Mapped[str] = mapped_column(primary_key=True, index=True)
    hash: Mapped[str]
    output_idx: Mapped[int]  # This is the index of output UTXO within this TX
    slot_no: Mapped[int]
    header_hash: Mapped[str]  # Required in case of Kupo restart
    index: Mapped[int]  # This is the index of TX within block, I think
    owner: Mapped[str]
    timestamp: Mapped[int]
    value: Mapped[str]  # JSONified string of attached value of tx output
    datum_hash: Mapped[str]  # hash of the attached output datum
    inline_datum: Mapped[bool]  # whether the datum is inline or not
    datum: Mapped[str] = mapped_column(nullable=True)  # CBOR hex of the attached output datum, if available
    order: Mapped["Order"] = relationship(back_populates="tx", cascade='all, delete')

    # There can be more swaps in a single tx, this corresponds to one swap
    # Therefore we require only that (tx hash + output utxo idx) is unique
    __table_args__ = (
        UniqueConstraint('hash', 'output_idx', name='unique_utxo'),
        Index("utxo_by_hash_and_idx", "hash", "output_idx"),
    )


class Order(Base):
    __tablename__ = "Order"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    dex_id: Mapped[str]
    tx_id: Mapped[int] = mapped_column(
        ForeignKey(Tx.id, ondelete="cascade", onupdate="cascade"), index=True
    )
    ask_token: Mapped[str]
    bid_token: Mapped[str]
    ask_amount: Mapped[Decimal]
    bid_amount: Mapped[Decimal]
    # how much of the asked amount was received so far
    fulfilled_amount: Mapped[Decimal]
    # how much of the bid amount was paid so far
    paid_amount: Mapped[Decimal]
    batcher_fee: Mapped[Decimal]
    # Part of fee that was paid until now in case of partial matches
    # paid_fee: Mapped[Decimal]
    lvl_deposit: Mapped[Decimal]

    sender_pkh: Mapped[str]
    sender_skh: Mapped[str]
    beneficiary_pkh: Mapped[str]
    beneficiary_skh: Mapped[str]
    status: Mapped[str]

    is_muesliswap: Mapped[bool]
    aggregator_platform: Mapped[str] = mapped_column(nullable=True)

    # By using the following relationship we avoid adding a new table
    cancellation_id: Mapped[int] = mapped_column(
        ForeignKey("Cancellation.id", ondelete="SET NULL", onupdate="cascade"),
        index=True, nullable=True
    )

    full_match_id: Mapped[int] = mapped_column(
        ForeignKey("FullMatch.id", ondelete="SET NULL", onupdate="cascade"),
        index=True, nullable=True
    )

    tx: Mapped[Tx] = relationship(back_populates="order")
    partial_matches: Mapped[List["PartialMatch"]] = relationship(
        back_populates="order", cascade="all, delete", order_by="PartialMatch.slot_no"
    )
    cancellation: Mapped["Cancellation"] = relationship(back_populates="order")
    full_match: Mapped["FullMatch"] = relationship(back_populates="order")

    def get_current_utxo(self):
        """returns the most recent orderbook utxo - either original or partial match"""
        if len(self.partial_matches) > 0:
            return self.partial_matches[-1].new_utxo
        return self.tx

    def get_status(self) -> OrderStatus:
        if self.cancellation_id is not None:
            return OrderStatus.CANCELLED
        if self.full_match_id is not None:
            return OrderStatus.FULFILLED
        if len(self.partial_matches) > 0:
            return OrderStatus.PARTIAL_MATCH
        return OrderStatus.OPEN

    def finalized_at(self) -> int | None:
        if self.cancellation is not None:
            return util.slot_datestring(self.cancellation.slot_no)
        if self.full_match is not None:
            return util.slot_datestring(self.full_match.slot_no)
        return None

    __table_args__ = (
        Index("order_sender_pkh", "sender_pkh"),
        Index("order_sender_skh", "sender_skh"),
    )


class PartialMatch(Base):
    __tablename__ = "PartialMatch"
    id: Mapped[int] = mapped_column(primary_key=True)
    order_id: Mapped[int] = mapped_column(ForeignKey(Order.id, ondelete="cascade", onupdate="cascade"))
    new_utxo_id: Mapped[str] = mapped_column(ForeignKey(Tx.id, ondelete="cascade", onupdate="cascade"))
    order: Mapped[Order] = relationship(
        back_populates="partial_matches", cascade="all, delete", foreign_keys=[order_id], uselist=False
    )
    new_utxo: Mapped[Tx] = relationship(cascade="all, delete", foreign_keys=[new_utxo_id], uselist=False)
    matched_amount: Mapped[Decimal]  # not cumulative
    paid_amount: Mapped[Decimal]  # not cumulative
    slot_no: Mapped[int]
    __table_args__ = (
        UniqueConstraint('order_id', 'new_utxo_id', name='unique_utxo_order_pm_pair'),
        Index("partial_match_by_order_utxo", "order_id", "new_utxo_id"),
        Index("partial_match_by_order_slot_no", "order_id", "slot_no"),
    )


class Cancellation(Base):
    __tablename__ = "Cancellation"
    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    order: Mapped[Order] = relationship(
        back_populates="cancellation", cascade="all, delete", uselist=False
    )
    # block in which the match occurred (kept here since we don't track the utxo)
    slot_no: Mapped[int]
    tx_hash: Mapped[str]


class FullMatch(Base):
    __tablename__ = "FullMatch"
    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    order: Mapped[Order] = relationship(
        back_populates="full_match", cascade="all, delete", uselist=False
    )
    matched_amount: Mapped[Decimal]  # not cumulative
    paid_amount: Mapped[Decimal]  # not cumulative
    # block in which the match occurred (kept here since we don't track the utxo)
    slot_no: Mapped[int]
    tx_hash: Mapped[str]


########################################################################################
#                      Helpers to get (and potentially create) Rows                    #
########################################################################################


def get_max_slot_block_and_index() -> tuple:
    """
    Return the largest slot-number and tx-index with an order
    in the database.
    """
    stmt = (
        sqla.select(Tx.slot_no, Tx.header_hash, Tx.index)
        .join(Tx.order)
        .order_by(Tx.slot_no.desc(), Tx.index.desc())
        .limit(1)
    )
    with Session(_ENGINE) as session:
        res = session.execute(stmt).first()
        session.rollback()
    if not res:
        return 0, "", 0
    return res  # type: ignore


Base.metadata.create_all(_ENGINE)
