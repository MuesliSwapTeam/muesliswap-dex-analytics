from typing import List

import sqlalchemy as sqla
from cardano_python_utils import classes  # type: ignore
from sqlalchemy import ForeignKey
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
    Session,
)
from sqlalchemy.schema import UniqueConstraint
from sqlalchemy.types import BigInteger
from sqlalchemy.engine import Engine
from sqlalchemy import event


@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    """This makes sure that sqlite will respect cascade delete"""
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


def create_engine(echo: bool = False) -> sqla.Engine:
    return sqla.create_engine("sqlite+pysqlite:///db.sqlite", echo=echo)


_ENGINE = create_engine(echo=False)


########################################################################################
#                                          DB Schema                                   #
########################################################################################


class Base(DeclarativeBase):
    pass


class DEX(Base):
    __tablename__ = "DEX"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    name: Mapped[str] = mapped_column(unique=True)

    orders: Mapped[List["Order"]] = relationship(back_populates="dex")


class Token(Base):
    __tablename__ = "Token"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    policy_id: Mapped[str]
    name: Mapped[str]

    ask_orders: Mapped[List["Order"]] = relationship(
        back_populates="ask_token", foreign_keys="Order.ask_token_id"
    )
    bid_orders: Mapped[List["Order"]] = relationship(
        back_populates="bid_token", foreign_keys="Order.bid_token_id"
    )


class Tx(Base):
    __tablename__ = "Tx"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    hash: Mapped[str]
    output_idx: Mapped[int]  # This is the index of output UTXO within this TX
    slot_no: Mapped[int]
    header_hash: Mapped[str]  # Required in case of Kupo restart
    index: Mapped[int]  # This is the index of TX within block, I think
    owner: Mapped[str]
    timestamp: Mapped[int]

    order: Mapped["Order"] = relationship(back_populates="tx", cascade="all, delete")

    # There can be more swaps in a single tx, this corresponds to one swap
    # Therefore we require only that (tx hash + output utxo idx) is unique
    __table_args__ = (UniqueConstraint("hash", "output_idx", name="unique_utxo"),)


class Order(Base):
    __tablename__ = "Order"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    dex_id: Mapped[int] = mapped_column(
        ForeignKey(DEX.id, ondelete="cascade", onupdate="cascade"), index=True
    )
    tx_id: Mapped[int] = mapped_column(
        ForeignKey(Tx.id, ondelete="cascade", onupdate="cascade"), index=True
    )
    ask_token_id: Mapped[int] = mapped_column(
        ForeignKey(Token.id, ondelete="cascade", onupdate="cascade"), index=True
    )
    bid_token_id: Mapped[int] = mapped_column(
        ForeignKey(Token.id, ondelete="cascade", onupdate="cascade"), index=True
    )
    ask_amount: Mapped[int] = mapped_column(BigInteger)
    bid_amount: Mapped[int] = mapped_column(BigInteger)
    fulfilled_amount: Mapped[int] = mapped_column(BigInteger, nullable=True)
    batcher_fee: Mapped[int]
    lvl_deposit: Mapped[int]

    sender_pkh: Mapped[str]
    sender_skh: Mapped[str]
    beneficiary_pkh: Mapped[str]
    beneficiary_skh: Mapped[str]
    status: Mapped[str]

    is_muesliswap: Mapped[bool]
    aggregator_platform: Mapped[str] = mapped_column(nullable=True)
    finalized_at: Mapped[int] = mapped_column(BigInteger, nullable=True)

    dex: Mapped[DEX] = relationship(back_populates="orders")
    tx: Mapped[Tx] = relationship(back_populates="order")
    ask_token: Mapped[Token] = relationship(
        back_populates="ask_orders", foreign_keys=[ask_token_id]
    )
    bid_token: Mapped[Token] = relationship(
        back_populates="bid_orders", foreign_keys=[bid_token_id]
    )


########################################################################################
#                      Helpers to get (and potentially create) Rows                    #
########################################################################################


def get_dex(name: str) -> DEX:
    """
    Obtain the DEX with the given name from the database
    and insert it if it is not already in the database.
    Returns a detached object that must first be added to a session.
    """
    stmt = sqla.select(DEX).where(DEX.name == name)
    with Session(_ENGINE) as session:
        row = session.scalar(stmt)
        if row is None:
            row = DEX(name=name)
            session.add(row)
            session.commit()
    return row


def get_token(token: classes.Token, session: Session) -> Token:
    """
    Obtain the given Token from the database and insert it if not already in there.
    Returns a detached object that must first be added to a session.
    """
    stmt = sqla.select(Token).where(
        Token.policy_id == token.policy_id, Token.name == token.name
    )
    row = session.scalar(stmt)
    if row is None:
        row = Token(policy_id=token.policy_id, name=token.name)
        session.add(row)
    return row


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
