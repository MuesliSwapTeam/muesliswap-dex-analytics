import logging
import sqlalchemy as sqla
from sqlalchemy.orm import Session
from .db import _ENGINE, Tx, get_max_slot_block_and_index

MAX_ALLOWED_ROLLBACK = 2 * 86400 / 20  # rollback at most 2 days of blocks
_LOGGER = logging.getLogger(__name__)


class RollbackHandler:
    def __init__(self):
        self.slot, self.block_hash, _ = get_max_slot_block_and_index()
        self.original_slot = self.slot
        _LOGGER.warning(f"Starting rollback from {self.slot}.{self.block_hash}")
        stmt = (
            sqla.select(Tx.slot_no, Tx.header_hash)
            .distinct()  # otherwise we'd revert to 1 block >1 times
            .order_by(Tx.slot_no.desc())
        )
        self.session = Session(_ENGINE)
        self.res = self.session.execute(stmt)
        self.res.fetchone()  # do away with current block

    def prev_block(self):
        row = self.res.fetchone()
        if (self.original_slot - self.slot) >= MAX_ALLOWED_ROLLBACK:
            raise Exception("Exceeded maximal rollback length - is the node synced?")
        if row is None:
            raise Exception("No more blocks to roll back!")

        self.slot, self.block_hash = row
        _LOGGER.warning(
            f"Rolled back {self.original_slot - self.slot} blocks, now at {self.slot}.{self.block_hash}"
        )
        return self.slot, self.block_hash

    def rollback(self):
        # delete everything newer than the block that we roll back to
        _LOGGER.warning(f"Executing rollback to block {self.slot}.{self.block_hash}")
        stmt = sqla.delete(Tx).where(Tx.slot_no > self.slot)
        self.session.execute(stmt)
        self.session.commit()
