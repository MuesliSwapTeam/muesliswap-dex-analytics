import argparse
import logging
import queue
import sys
import threading

import querier.config as config
import querier.db as db
from querier import BlockIterator
from querier.counter import VolumeCounter
from querier.ogmios import OgmiosIterator
from querier.patterns import update_patterns
from querier.rollback import RollbackHandler

_LOGGER = logging.getLogger(__name__)


class SynchronizedIterator(BlockIterator):
    def __init__(self):
        self.queue = []  # or we could use queue.Queue()
        self.lock = threading.Lock()
        self.has_updated = threading.Condition()
        self.should_exit = False

    def submit_block(self, block):
        with self.lock:
            self.queue.append(block)
            if len(self.queue) > 1000:
                # i think this should happen but still
                _LOGGER.warning(f"QUERIER IS LAGGING BEHIND! {len(self.queue)}")
        with self.has_updated:
            self.has_updated.notify()

    def iterate_blocks(self):
        while not self.should_exit:
            with self.has_updated:
                self.has_updated.wait(timeout=10)
            with self.lock:
                n = len(self.queue)
            for i in range(n):
                with self.lock:
                    block = self.queue.pop(0)
                yield block


def prepare_database():
    # On start, we always rollback by one block, since it may have
    # been incompletely processed when the server last exited
    start_slot_no, start_block_hash, _ = db.get_max_slot_block_and_index()
    if start_slot_no > 0:
        rollback_handler = RollbackHandler()
        rollback_handler.prev_block()
        rollback_handler.rollback()

    # Now we find out what's the actual block that we should process first
    start_slot_no, start_block_hash, _ = db.get_max_slot_block_and_index()
    if start_slot_no == 0 or not start_block_hash:
        start_slot_no = config.DEFAULT_START_SLOT
        start_block_hash = config.DEFAULT_START_HASH

    return start_slot_no, start_block_hash


def run_as_single_thread():
    start_slot_no, start_block_hash = prepare_database()

    ogmios = OgmiosIterator()
    ogmios.init_connection(start_slot_no, start_block_hash)

    counter = VolumeCounter(
        ogmios=ogmios,
        update_pool_addrs=update_patterns,
    )

    try:
        counter.run()
    except Exception as ex:
        _LOGGER.exception("Exception in main loop")
        raise ex


def _run_analytics_async(iterator: SynchronizedIterator):
    try:
        analytics = VolumeCounter(
            ogmios=iterator,
            update_pool_addrs=update_patterns,
        )
        analytics.run()
    except Exception:
        raise Exception("Exception in analytics thread")
    iterator.should_exit = True


def _run_ogmios_async(start_slot_no, start_block_hash, iterator: SynchronizedIterator):
    try:
        ogmios = OgmiosIterator()
        ogmios.init_connection(start_slot_no, start_block_hash)
        block_generator = ogmios.iterate_blocks()
        for block in block_generator:
            iterator.submit_block(block)
            if iterator.should_exit:
                break
    except Exception as ex:
        _LOGGER.exception("Exception in Ogmios thread")
    iterator.should_exit = True


def run_as_multiple_threads():
    start_slot_no, start_block_hash = prepare_database()

    iterator = SynchronizedIterator()

    t1 = threading.Thread(target=_run_analytics_async, args=(iterator,))
    t2 = threading.Thread(target=_run_ogmios_async, args=(start_slot_no, start_block_hash, iterator))

    t1.start()
    t2.start()
    t1.join()
    t2.join()


if __name__ == "__main__":
    argp = argparse.ArgumentParser()
    argp.add_argument("--multithreaded", action='store_true', default=False)  # experimental
    args = argp.parse_args()
    if args.multithreaded:
        run_as_multiple_threads()
    else:
        run_as_single_thread()
