import json
import time
import logging

import websocket
import base64

from .config import OGMIOS_URL
from .secret import OGMIOS_SERVER_USERNAME, OGMIOS_SERVER_PASSWORD
from .rollback import RollbackHandler

_LOGGER = logging.getLogger(__name__)

TEMPLATE = {
    "jsonrpc": "2.0",
}

NEXT_BLOCK = TEMPLATE.copy()
NEXT_BLOCK["method"] = "nextBlock"
NEXT_BLOCK = json.dumps(NEXT_BLOCK)


class OgmiosIterator:

    def __init__(self, dex_addrs: dict = None):
        self.dex_addrs = dex_addrs

    def init_connection(self, start_slot, start_hash):
        self.start_slot = start_slot
        self.start_hash = start_hash

        self.ws = websocket.WebSocket()
        headers = {}
        if OGMIOS_SERVER_USERNAME is not None and OGMIOS_SERVER_PASSWORD is not None:
            credentials = f"{OGMIOS_SERVER_USERNAME}:{OGMIOS_SERVER_PASSWORD}"
            encoded_credentials = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")
            headers["Authorization"] = f"Basic {encoded_credentials}"
        try:
            self.ws.connect(OGMIOS_URL, header=headers)
        except websocket.WebSocketBadStatusException as e:
            raise Exception(f"Can't connect to Ogmios server on {OGMIOS_URL}") from e

        data = TEMPLATE.copy()
        data["method"] = "findIntersection"
        data["params"] = {
            "points": [{"slot": start_slot, "id": start_hash}],
        }

        _LOGGER.info(f"FindIntersect, setting last block to: {start_slot}.{start_hash}")
        self.ws.send(json.dumps(data))
        resp = json.loads(self.ws.recv())
        # Example in case of rollback: 01]: [2023-09-25 11:21:02,061] INFO     querier.ogmios {'IntersectionNotFound': {'tip': {'slot': 104074568, 'hash': '82>
        if "No intersection found." in str(resp):
            # Rollback: We need to find the last common ancestor block (i believe this can't be more than the security parameter, so we can just iterate backwards until we find it)
            rollback_handler = RollbackHandler()
            while True:
                slot, block_hash = rollback_handler.prev_block()
                data["params"]["points"][0] = {"slot": slot, "id": block_hash}
                self.ws.send(json.dumps(data))
                resp = json.loads(self.ws.recv())
                if "No intersection found." not in str(resp):
                    rollback_handler.rollback()
                    break
        self.ws.send(NEXT_BLOCK)
        self.ws.recv()  # this just says roll back to the intersection (we already did)

    def iterate_blocks(self, start_slot=None, start_hash=None):
        if start_slot is None or start_hash is None:
            self.init_connection(self.start_slot, self.start_hash)
        else:
            self.init_connection(start_slot, start_hash)
        # we want to always keep 100 blocks in queue to avoid waiting for node
        for i in range(100):
            self.ws.send(NEXT_BLOCK)
        while True:
            resp = self.ws.recv()
            resp = json.loads(resp)
            if "result" not in resp or "backward" in resp["result"]:
                raise Exception(
                    "Ogmios Rollback!"
                )  # this will restart querier and trigger rollback above
            self.ws.send(NEXT_BLOCK)
            yield resp


"""
{
  "type": "jsonwsp/response",
  "version": "1.0",
  "servicename": "ogmios",
  "method": "nextBlock",
  "result": {
    "RollBackward": {
      "point": {
        "slot": 102855375,
        "id": "a3294a2ac1794454dcb3d6cc574453b2dfbe3ad692b33ae9fc41ef9e69b83b70"
      },
      "tip": {
        "slot": 102855385,
        "id": "66ae664850360836feab69f8ce814b6374059e6de183e9bad97c4f7f9a63faa0",
        "blockNo": 9275075
      }
    }
  },
  "reflection": null
}
"""
