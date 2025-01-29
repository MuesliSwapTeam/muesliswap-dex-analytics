import os

from cardano_python_utils.classes import Bech32Addr

VYFI_POOLS_ENDPOINT = "https://api.vyfi.io/lp?networkId=1"
VYFI_LVL_ATTACHED = 2000000
MINSWAP_ORDER_CONTRACT = Bech32Addr("addr1zxn9efv2f6w82hagxqtn62ju4m293tqvw0uhmdl64ch8uw6j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq6s3z70")
MINSWAP_ORDER_CONTRACT_NOSTAKE = Bech32Addr("addr1wxn9efv2f6w82hagxqtn62ju4m293tqvw0uhmdl64ch8uwc0h43gt")
MINSWAP_V0_ORDER_CONTRACT = Bech32Addr("addr1wyx22z2s4kasd3w976pnjf9xdty88epjqfvgkmfnscpd0rg3z8y6v")
MINSWAP_POOL_CONTRACT = Bech32Addr("addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha")
WINGRIDERS_ORDER_CONTRACT = Bech32Addr("addr1wxr2a8htmzuhj39y2gq7ftkpxv98y2g67tg8zezthgq4jkg0a4ul4")
SPLASH_ORDER_CONTRACT = Bech32Addr("addr1qygtklglhrru2y4hja2h09d3ejtvy2ekxnyk9w47cuhmpukqyj9dyzepjvsggu79qd5qn2n8y3eqrwh8gqjvasugtscq0yguq0")
SPLASH_POOL_CONTRACT = Bech32Addr("addr1xxw7upjedpkr4wq839wf983jsnq3yg40l4cskzd7dy8eyndj764lvrxdayh2ux30fl0ktuh27csgmpevdu89jlxppvrsgddq74")
SUNDAE_ORDER_CONTRACT = "addr1wxaptpmxcxawvr3pzlhgnpmzz3ql43n2tc8mn3av5kx0yzs09tqh8"
MUESLISWAP_POOLS_API_URL = "https://api.muesliswap.com/liquidity/pools"

# For debugging
# DEFAULT_START_SLOT = 120658977 # 46536192
# DEFAULT_START_HASH = "0692feccd84689936ecaa07902f2e23bd8493293e1751d031af6d0ef5c33f794" # "f90517fd9fb9194b1ef6f9c1d147c0f175c3b29604542692e3d06bf13403aacf"

DEFAULT_START_SLOT = 46536192
DEFAULT_START_HASH = "f90517fd9fb9194b1ef6f9c1d147c0f175c3b29604542692e3d06bf13403aacf"


OGMIOS_URL = os.environ.get("OGMIOS_URL", "ws://localhost:1337")
