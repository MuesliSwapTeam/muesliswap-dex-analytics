import unittest

# root
# - part 1 of datum
# -- 1. address obj
# --- {something}
# -- 2. pubkeyhash (hex str)
# -- 3. unix timestamp (int)
# -- 4. tokens obj
# --- {something}
# - part 2 of datum
# -- 1. swap direction: empty constructor with (0/1) constructor
# -- 2. buy amount (int)

# tokens a, b should be lexicographically ordered
# a -> b direction 0, b -> a direction 1
# ADA is always token a because of the ordering

# Swapping empowa -> ada: direction is 1
# Swapping ada -> milk: direction is 0


TEST_DATUM = \
{"constructor": 0, "fields": [  # root
    {"constructor": 0, "fields": [  # part 1 of datum
        {"constructor": 0, "fields": [  # 1. address_obj
            {"constructor": 0, "fields": [
                "0x353b8bc29a15603f0b73eac44653d1bd944d92e0e0dcd5eb185164a2"
            ]},
            {"constructor": 0, "fields": [ # note: this is wrong, "bytes" missing
                {"constructor": 0, "fields": [{"constructor": 0, "fields": ["0xda22c532206a75a628778eebaf63826f9d93fbe9b4ac69a7f8e4cd78"]}]}
            ]}]},
        "0x353b8bc29a15603f0b73eac44653d1bd944d92e0e0dcd5eb185164a2",  # 2. pubkeyhash
        1687277612523,  # 3. unix timestamp
        {"constructor": 0, "fields": [  # 4. tokens obj
            {"constructor": 0, "fields": ["", ""]},
            {"constructor": 0, "fields": ["0x8a1cfae21368b8bebbbed9800fec304e95cce39a2a57dc35e2e3ebaa", "MILK"]}
        ]}
    ]},
    {"constructor": 0, "fields": [  # part 2 of datum
        {"constructor": 0, "fields": []},  # swap direction (constructor 0/1)
        1  # buy amount in tokens
    ]}
]}


class MyTestCase(unittest.TestCase):
    def test_sundae_datum(self):
        from . import parse_swap_datum
        swap_data = parse_swap_datum(TEST_DATUM)
        print(swap_data)
        self.assertEqual(True, True)  # add assertion here


if __name__ == '__main__':
    unittest.main()
