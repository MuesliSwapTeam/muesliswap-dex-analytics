import os
import pathlib

# HTTP parameter names for order parameters
A_POLICYID_ARG: str = "pid-a"
A_TOKENNAME_ARG: str = "tname-a"
B_POLICYID_ARG: str = "pid-b"
B_TOKENNAME_ARG: str = "tname-b"
TOKEN_SUBJECT_ARG = "token-subject"
WALLET_ARG: str = "wallet"
FROM_ARG = "time-from"
SENDER_PKH_ARG = "sender-pkh"
SENDER_SKH_ARG = "sender-skh"
LIMIT_ARG = "limit"
OFFSET_ARG = "offset"

# default timeout for cached value in seconds
CACHE_DEFAULT_TIMEOUT = 300

ROOT_DIR = pathlib.Path(__file__).parent.parent
DATA_DIR = pathlib.Path(os.environ.get("DATA_DIR", ROOT_DIR / "data"))
DATA_DIR.mkdir(exist_ok=True, parents=True)

DATABASE_URI = os.environ.get("DATABASE_URI", f"sqlite+pysqlite:///{ROOT_DIR / 'db.sqlite'}")
