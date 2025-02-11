import datetime
import itertools
import math
import sys
import logging
from collections import defaultdict
from math import fsum

import sqlalchemy
from sqlalchemy.sql import text

from . import DATABASE_URI

from typing import TypedDict, List, Optional
import statistics

# Global DB engine
engine = sqlalchemy.create_engine(DATABASE_URI)
IS_POSTGRES = "postgresql" in DATABASE_URI
_LOGGER = logging.getLogger(__name__)
_LOGGER.setLevel(logging.INFO)


def slot_timestamp(slot_no: int) -> int:
    return 1596491091 + (slot_no - 4924800)

def timestamp_slot(timestamp: int) -> int:
    return (timestamp - 1596491091) + 4924800

def slot_timestamp_sql(slot_no_var: str) -> str:
    return f"1596491091 + ({slot_no_var} - 4924800)"

def timestamp_slot_sql(timestamp_var: str) -> str:
    return f"({timestamp_var} - 1596491091) + 4924800"


def slot_datestring(slot_no: int) -> str:
    return datetime.datetime.utcfromtimestamp(slot_timestamp(slot_no)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def filter_by_dex(sql_params: dict, dex_id: str = None, aggregator_id: str = None, table_name='"Order"'):
    dex_condition = " "
    if dex_id:
        dex_condition += f' and {table_name}.dex_id = :dex_id '
        sql_params["dex_id"] = dex_id
    if aggregator_id:
        dex_condition += f' and {table_name}.aggregator_platform = :aggregator_id '
        sql_params["aggregator_id"] = aggregator_id
    return dex_condition


def get_current_volume(
    policy_id_a=None,
    tokenname_a=None,
    policy_id_b=None,
    tokenname_b=None,
    time_from:int=None,
    time_to:int=None,
    dex_id: str = None,
    aggregator_id: str = None,
):
    if time_from is None:
        dt_from = datetime.datetime.now() - datetime.timedelta(days=1)
        time_from = dt_from.timestamp()
    conditions = '"slot_no" >= :slot_from'
    params = {"slot_from": timestamp_slot(time_from)}
    if time_to is not None:
        conditions += " AND slot_no <= :slot_to"
        params["slot_to"] = timestamp_slot(time_to)

    if policy_id_a is not None and tokenname_a is not None:
        params["token_a"] = policy_id_a + tokenname_a
        if policy_id_b is not None and tokenname_b is not None:
            # filtering by two tokens
            conditions += " AND ((ask_token = :token_a AND bid_token = :token_b) OR" \
                        " (ask_token = :token_b AND bid_token = :token_a))"
            params["token_b"] = policy_id_b + tokenname_b
        else:
            # filtering by only one token
            conditions += " AND ((ask_token = :token_a) OR (bid_token = :token_a))"
    # otherwise return all volume data

    dex_condition = filter_by_dex(params, dex_id, aggregator_id)

    # date_label and date_fmt are only used internally, otherwise we shouldn't use f strings here
    query = f'''\
        with matches as (SELECT
            "FullMatch".matched_amount as matched_amount,
            "FullMatch".paid_amount as paid_amount,
            "FullMatch".slot_no as slot_no,
            "Order".ask_token as ask_token,
            "Order".bid_token as bid_token,
            "Order".dex_id as dex_id
            from "Order" inner join "FullMatch" on "Order".full_match_id = "FullMatch".id
            {dex_condition}
            UNION SELECT 
            "PartialMatch".matched_amount as matched_amount,
            "PartialMatch".paid_amount as paid_amount,
            "PartialMatch".slot_no as slot_no,
            "Order".ask_token as ask_token,
            "Order".bid_token as bid_token,
            "Order".dex_id as dex_id
            from "PartialMatch" join "Order" on "PartialMatch".order_id = "Order".id
            {dex_condition}
        )
        SELECT 
            m.dex_id as dex,
            m.ask_token as ask_token,
            m.bid_token as bid_token,
            SUM( m.matched_amount ) AS volume_a,
            SUM( m.paid_amount ) AS volume_b,
            COUNT(*) AS trades
        FROM matches m
        WHERE {conditions}
        GROUP BY dex, m.ask_token, m.bid_token;
    '''

    results = {}

    with engine.connect() as con:
        sql_query = sqlalchemy.text(query)
        for row in con.execute(sql_query, params):
            result = {}
            sql_names = ["dex", "token_a", "token_b", "volume_a", "volume_b", "trades"]
            for i, name in enumerate(sql_names):
                result[name] = row[i]

            dex = result["dex"]
            ask = result["token_a"]
            bid = result["token_b"]
            if ask < bid:  # -> ada is always token b
                result_id = f'{dex}_{ask}_{bid}'
                # Mirror all fields so that we can merge properly
                result["token_a"], result["token_b"] = result["token_b"], result["token_a"]
                result["volume_a"], result["volume_b"] = result["volume_b"], result["volume_a"]
            else:
                result_id = f'{dex}_{bid}_{ask}'

            if result_id in results:
                # We don't need to update dex, pids, tnames since they're in id
                results[result_id]["volume_a"] += result["volume_a"]
                results[result_id]["volume_b"] += result["volume_b"]
            else:
                results[result_id] = result

    return list(results.values())


def get_order_history(sender_pkh: str, sender_skh: str,
                      dex_id: str = None, aggregator_id: str = None,
                      limit: int = 100, offset: int = 0):
    # note that both keys may be empty to allow for custom queries
    condition = "TRUE"
    if sender_pkh is not None:
        condition += " and sender_pkh = :sender_pkh"
    if sender_skh is not None:
        condition += " and sender_skh = :sender_skh"
    if sender_pkh is None and sender_skh is None:
        raise ValueError("At least one of sender_pkh, sender_skh must be set")

    params = {"limit": limit, "offset": offset, "sender_pkh": sender_pkh, "sender_skh": sender_skh}
    condition += filter_by_dex(params, dex_id, aggregator_id)

    query = f"""
    with last_partial_match as (
        select order_id, max(slot_no) as slot_no
        from "PartialMatch"
        group by order_id
    )
    SELECT 
    o.ask_amount,
    o.bid_amount,
    o.paid_amount,
    o.fulfilled_amount,
    o.batcher_fee,
    o.lvl_deposit,
    o.ask_token,
    o.bid_token,
    o.dex_id,
    -- automatically determines last relevant index, tx hash and slot_no
    {slot_timestamp_sql("t.slot_no")},
    {slot_timestamp_sql("coalesce(f.slot_no, c.slot_no)")} as final_slot_no,
    coalesce(f.tx_hash, c.tx_hash, pm_tx.hash, t.hash),
    coalesce(pm_tx.output_idx, t.output_idx),
    o.aggregator_platform,
    o.beneficiary_pkh,
    o.beneficiary_skh,
    -- use the below values to compute status
    o.cancellation_id as canceled,
    o.full_match_id as fulfilled,
    lpm.slot_no as last_partial_slot_no
    FROM "Order" o 
    JOIN "Tx" t ON o.tx_id = t.id
    LEFT OUTER JOIN FullMatch f ON o.full_match_id = f.id
    LEFT OUTER JOIN Cancellation c ON o.cancellation_id = c.id
    LEFT OUTER JOIN last_partial_match lpm ON o.id = lpm.order_id
    LEFT OUTER JOIN "PartialMatch" pm ON lpm.order_id = pm.order_id AND lpm.slot_no = pm.slot_no
    LEFT OUTER JOIN "Tx" pm_tx ON pm.new_utxo_id = pm_tx.id
    WHERE {condition}
    ORDER BY final_slot_no DESC
    LIMIT :limit OFFSET :offset
    """
    objects = []
    fields = [
        "toAmount",
        "fromAmount",
        "paidAmount",
        "receivedAmount",
        "batcherFee",
        "attachedLvl",
        "toToken",
        "fromToken",
        "dex",
        "placedAt",
        "finalizedAt",
        "txHash",
        "outputIdx",
        "aggregatorPlatform",
        "pubKeyHash",
        "stakeKeyHash",
    ]

    with engine.connect() as con:
        sql_query = sqlalchemy.text(query)
        for row in con.execute(sql_query, params):
            obj = {k: v for k, v in zip(fields, row)}
            obj["status"] = "matched" if row[16] is not None else "canceled" if row[17] is not None else "partial" if row[18] is not None else "open"
            objects.append(obj)
    return objects


def get_volume_by_dex(subject: str, time_from: float, aggregator_id: str = None):
    """
    Counts total volume of all matched orders involving the token [subject] (as amount of this token).
    Optionally, only orders from a specific aggregator can be counted.
    """

    params = {"subject": subject, "time_from": time_from}
    condition = filter_by_dex(params, dex_id=None, aggregator_id=aggregator_id)

    query_ask = f"""SELECT
    SUM("Order".fulfilled_amount) AS volume,
    "Order".dex_id AS dex
    FROM "Order"
    JOIN "Tx" on "Order".tx_id = "Tx".id
    WHERE
        "Order".ask_token = :subject
        AND "Tx".timestamp >= :time_from
        AND "Order".full_match_id is not NULL
        {condition}
    GROUP BY dex
    """

    query_bid = f"""SELECT
    SUM("Order".bid_amount) AS volume,
    "Order".dex_id AS dex
    FROM "Order"
    JOIN "Tx" on "Order".tx_id = "Tx".id
    WHERE
        "Order".bid_token = :subject
        AND "Tx".timestamp >= :time_from
        AND "Order".full_match_id is not NULL
        {condition}
    GROUP BY dex
    """

    result = {}

    with engine.connect() as con:
        for query in [query_ask, query_bid]:
            sql_query = sqlalchemy.text(query)
            for row in con.execute(sql_query, params):
                # sql_names = ["volume", "dex"]
                sum_amount, dex_name = row
                result[dex_name] = result.get(dex_name, 0) + sum_amount
    return result


def get_volume_chart(base_token: str, quote_token: str, interval: int, dex_id=None, aggregator_id=None, limit: int = 100):

    params = {"base_token": base_token, "quote_token": quote_token, "limit": limit, "interval": interval}
    dex_condition = filter_by_dex(params, dex_id, aggregator_id)

    subquery = f"""
    (SELECT
    "Order".bid_amount as amount,
    "Tx".timestamp - MOD("Tx".timestamp, :interval) as time
    from "Order"
     join "Tx" on "Tx".id = "Order".tx_id
     where
        ("Order".bid_token = :base_token and "Order".ask_token = :quote_token) and
        "Order".full_match_id is not null {dex_condition}
    UNION SELECT
    "Order".fulfilled_amount as amount,
    "Tx".timestamp - MOD("Tx".timestamp, :interval) as time
    from "Order"
     join "Tx" on "Tx".id = "Order".tx_id
     where
        ("Order".ask_token = :base_token and "Order".bid_token = :quote_token) and
        "Order".full_match_id is not null {dex_condition}
    )
    """
    query = """
    SELECT sum(amount), time from ({subquery}) group by time order by time desc limit :limit
    """.format(subquery=subquery)

    result = []

    with engine.connect() as con:
        sql_query = sqlalchemy.text(query)
        fields = ['baseVolume', 'timestamp']
        for values in con.execute(sql_query, params):
            d = {k: v for k, v in zip(fields, values)}
            d['timestamp'] = datetime.datetime.utcfromtimestamp(d['timestamp']).isoformat()
            result.append(d)
    return result

MILKv2 = "afbe91c0b44b3040e360057bf8354ead8c49c4979ae6ab7c4fbdc9eb4d494c4b7632"
MILKv1 = "8a1cfae21368b8bebbbed9800fec304e95cce39a2a57dc35e2e3ebaa4d494c4b"
MILKv2_launchdate = int(datetime.datetime(2024, 1, 26, tzinfo=datetime.timezone.utc).timestamp())

def correct_milkv1_ada_to_milkv2(x):
    factor = 1_000_000
    return {
        "datetime": x["datetime"],
        "time": x["time"],
        "start": x["start"],
        "end": x["end"],
        "median": x["median"] * factor,
        "mean": x["mean"] * factor,
        "open": x["open"] * factor,
        "close": x["close"] * factor,
        "high": x["high"] * factor,
        "low": x["low"] * factor,
        "volume_base": x["volume_base"] * factor,
        "volume_quote": x["volume_quote"],
    }


def correct_ada_milkv1_to_milkv2(x):
    factor = 1_000_000
    return {
        "datetime": x["datetime"],
        "time": x["time"],
        "start": x["start"],
        "end": x["end"],
        "median": x["median"] / factor,
        "mean": x["mean"] / factor,
        "open": x["open"] / factor,
        "close": x["close"] / factor,
        "high": x["high"] / factor,
        "low": x["low"] / factor,
        "volume_base": x["volume_base"],
        "volume_quote": x["volume_quote"] * factor,
    }

def weighted_mean(data, weights):
    """Convert data to floats and compute the arithmetic mean.
    Copied from 3.12 stdlib

    This runs faster than the mean() function and it always returns a float.
    If the input dataset is empty, it raises a StatisticsError.

    >>> weighted_mean([3.5, 4.0, 5.25])
    4.25
    """
    if not isinstance(weights, (list, tuple)):
        weights = list(weights)
    try:
        # it could also be decimal.Decimal -> float
        num = fsum(float(a) * float(b) for a, b in zip(data, weights))
    except ValueError:
        raise statistics.StatisticsError('data and weights must be the same length')
    den = fsum(weights)
    if not den:
        raise statistics.StatisticsError('sum of weights must be non-zero')
    return num / den

def weighted_median(data, weights):
    """
    Calculate the weighted median of a list.
    Copied from weightedstats
    """
    midpoint = 0.5 * fsum(weights)
    if any([j > midpoint for j in weights]):
        return data[weights.index(max(weights))]
    if any([j > 0 for j in weights]):
        sorted_data, sorted_weights = zip(*sorted(zip(data, weights)))
        cumulative_weight = 0
        below_midpoint_index = 0
        while cumulative_weight <= midpoint:
            below_midpoint_index += 1
            cumulative_weight += sorted_weights[below_midpoint_index-1]
        cumulative_weight -= sorted_weights[below_midpoint_index-1]
        if abs(cumulative_weight - midpoint) < sys.float_info.epsilon:
            bounds = sorted_data[below_midpoint_index-2:below_midpoint_index]
            return fsum(bounds) / float(len(bounds))
        return sorted_data[below_midpoint_index-1]


def get_price_and_volume_chart(
    interval: int,
    base_token: Optional[str] = None,
    quote_token: Optional[str] = None,
    dex_id: Optional[str] = None,
    aggregator_id: Optional[str] = None,
    from_timestamp: Optional[int] = None,
    to_timestamp: Optional[int] = None,
):
    """
    Generate the price and volume chart for all token pairs.
    Set base or quote token to filter by a specific token.

    """
    params = {}

    token_condition = ''
    if base_token:
        token_condition += f' and m.ask_token = :token_a '
        params['token_a'] = base_token
    if quote_token:
        token_condition += f' and m.bid_token = :token_b '
        params['token_b'] = quote_token

    dex_condition = filter_by_dex(params, dex_id, aggregator_id, table_name='m')

    if from_timestamp is not None:
        dex_condition += f' and m.slot_no >= :from_slot '
        params['from_slot'] = timestamp_slot(from_timestamp)
    if to_timestamp is not None:
        dex_condition += f' and m.slot_no <= :to_slot '
        params['to_slot'] = timestamp_slot(to_timestamp)

    query = f"""
    with matches as (SELECT
        "FullMatch".matched_amount as matched_amount,
        "FullMatch".paid_amount as paid_amount,
        "FullMatch".slot_no as slot_no,
        "Order".ask_token as ask_token,
        "Order".bid_token as bid_token,
        "Order".dex_id as dex_id,
        "Order".aggregator_platform as aggregator_platform
        from "Order" inner join "FullMatch" on "Order".full_match_id = "FullMatch".id
        UNION SELECT 
        "PartialMatch".matched_amount as matched_amount,
        "PartialMatch".paid_amount as paid_amount,
        "PartialMatch".slot_no as slot_no,
        "Order".ask_token as ask_token,
        "Order".bid_token as bid_token,
        "Order".dex_id as dex_id,
        "Order".aggregator_platform as aggregator_platform
        from "PartialMatch" join "Order" on "PartialMatch".order_id = "Order".id
        UNION SELECT  -- mirror all orders so the reverse appears in the same group
        "FullMatch".paid_amount as matched_amount,
        "FullMatch".matched_amount as paid_amount,
        "FullMatch".slot_no as slot_no,
        "Order".bid_token as ask_token,
        "Order".ask_token as bid_token,
        "Order".dex_id as dex_id,
        "Order".aggregator_platform as aggregator_platform
        from "Order" inner join "FullMatch" on "Order".full_match_id = "FullMatch".id
        UNION SELECT 
        "PartialMatch".paid_amount as matched_amount,
        "PartialMatch".matched_amount as paid_amount,
        "PartialMatch".slot_no as slot_no,
        "Order".bid_token as ask_token,
        "Order".ask_token as bid_token,
        "Order".dex_id as dex_id,
        "Order".aggregator_platform as aggregator_platform
        from "PartialMatch" join "Order" on "PartialMatch".order_id = "Order".id
    )

    select * from matches m
    where TRUE
    -- cleanup
    AND (m.ask_token != '' OR m.matched_amount >= 1000000)
    AND (m.bid_token != '' OR m.paid_amount >= 1000000)
    AND m.matched_amount > 0
    AND m.paid_amount > 0
    and not (m.bid_token = '8db269c3ec630e06ae29f74bc39edd1f87c819f1056206e879a1cd61446a65644d6963726f555344' and m.ask_token = '' and m.matched_amount >= 5 * m.paid_amount)
    and not (m.ask_token = '8db269c3ec630e06ae29f74bc39edd1f87c819f1056206e879a1cd61446a65644d6963726f555344' and m.bid_token = '' and m.paid_amount >= 5 * m.matched_amount)
    and not (m.bid_token = 'f66d78b4a3cb3d37afa0ec36461e51ecbde00f26c8f0a68f94b6988069555344' and m.ask_token = '' and m.matched_amount >= 5 * m.paid_amount)
    and not (m.ask_token = 'f66d78b4a3cb3d37afa0ec36461e51ecbde00f26c8f0a68f94b6988069555344' and m.bid_token = '' and m.paid_amount >= 5 * m.matched_amount)
    and not (m.bid_token = '8db269c3ec630e06ae29f74bc39edd1f87c819f1056206e879a1cd61446a65644d6963726f555344' and m.ask_token = 'f66d78b4a3cb3d37afa0ec36461e51ecbde00f26c8f0a68f94b6988069555344' and (m.matched_amount >= 1.5 * m.paid_amount or m.paid_amount >= 1.5 * m.matched_amount))
    and not (m.ask_token = '8db269c3ec630e06ae29f74bc39edd1f87c819f1056206e879a1cd61446a65644d6963726f555344' and m.bid_token = 'f66d78b4a3cb3d37afa0ec36461e51ecbde00f26c8f0a68f94b6988069555344' and (m.matched_amount >= 1.5 * m.paid_amount or m.paid_amount >= 1.5 * m.matched_amount))
    -- end cleanup
    {token_condition}
    {dex_condition}
    order by m.ask_token, m.bid_token, slot_no asc
    """

    res_map = defaultdict(list)
    with engine.connect() as con:
        sql_query = sqlalchemy.text(query)
        cur_timestamp_cut = 0
        cur_amts = []
        cur_token_a, cur_token_b = None, None
        for values in itertools.chain(con.execute(sql_query, params), ((0, 0, 0, 0, 0, 0, 0),)):
            matched_amount, paid_amount, slot_no, ask_token, bid_token, _, _ = values
            unix_timestamp = slot_timestamp(slot_no)
            unix_timestamp_cut = unix_timestamp // interval
            if cur_timestamp_cut != unix_timestamp_cut or cur_token_a != ask_token or cur_token_b != bid_token:
                if cur_amts:
                    cur_prices = [x[0] / x[1] for x in cur_amts]
                    frame = cur_timestamp_cut * interval
                    weights = [math.sqrt(x[0]*x[1]) for x in cur_amts]
                    cur_result = {
                        "datetime": datetime.datetime.utcfromtimestamp(frame).isoformat(),
                        "time": frame,
                        "start": frame,
                        "end": frame + interval,
                        "median": weighted_median(cur_prices, weights),
                        "mean": weighted_mean(cur_prices, weights),
                        "open": cur_prices[0],
                        "close": cur_prices[-1],
                        "high": max(cur_prices),
                        "low": min(cur_prices),
                        "volume_base": sum([x[0] for x in cur_amts]),
                        "volume_quote": sum([x[1] for x in cur_amts]),
                    }
                    res_map[(cur_token_a, cur_token_b)].append(cur_result)
                cur_timestamp_cut = unix_timestamp_cut
                cur_token_a, cur_token_b = ask_token, bid_token
                cur_amts = []
            cur_amts.append(
                (matched_amount, paid_amount)
            )
    if base_token == MILKv2 or base_token is None:
        # prepend the MILKv1 trading history to MILKv2
        quote_tokens = set([x[1] for x in res_map.keys()])
        new_chart = get_price_and_volume_chart(
            interval, MILKv1, quote_token, dex_id, aggregator_id,
            from_timestamp=from_timestamp,
            to_timestamp=min(MILKv2_launchdate, to_timestamp) if to_timestamp is not None else None
        )
        for quote_token in quote_tokens:
            res_map[MILKv2, quote_token] = list(map(
                correct_milkv1_ada_to_milkv2,
                new_chart[MILKv1, quote_token]
            )) + res_map[MILKv2, quote_token]
    if quote_token == MILKv2 or quote_token is None:
        # prepend the MILKv1 trading history to MILKv2
        base_tokens = set([x[0] for x in res_map.keys()])
        new_chart = get_price_and_volume_chart(
            interval, base_token, MILKv1, dex_id, aggregator_id,
            from_timestamp=from_timestamp,
            to_timestamp=min(MILKv2_launchdate, to_timestamp) if to_timestamp is not None else None
        )
        for base_token in base_tokens:
            res_map[base_token, MILKv2] = list(map(
                correct_ada_milkv1_to_milkv2,
                new_chart[base_token, MILKv1]
            )) + res_map[base_token, MILKv2]
    return res_map


def get_token_price_and_volume_chart(
        base_token: str,
        quote_token: str,
        interval: int,
        dex_id: Optional[str] = None,
        aggregator_id: Optional[str] = None,
        from_timestamp: Optional[int] = None,
        to_timestamp: Optional[int] = None,
):
    """
    Generate the price and volume chart for a specific token pair.
    """
    result = get_price_and_volume_chart(
        interval, base_token, quote_token, dex_id, aggregator_id, from_timestamp, to_timestamp
    )[base_token, quote_token]

    return result


def get_trades(token_from: str, token_to: str, limit: int, offset: int, dex_id: str = None, aggregator_id: str = None, min_ask_amount: int = None, min_bid_amount: int = None):
    """Returns a list of last completed trades between a<->b, regardless of swap direction,
    with optional filtering for high-value trades based on ask and bid amounts."""
    substring_expr = "POSITION('#' in \"PartialMatch\".new_utxo_id)" if IS_POSTGRES else "INSTR(\"PartialMatch\".new_utxo_id, '#')"
    params = {"token_a": token_from, "token_b": token_to, "limit": limit, "offset": offset}
    dex_condition = filter_by_dex(params, dex_id, aggregator_id, table_name='m')
    
    # Build extra conditions for high-value filtering if provided.
    extra_conditions = ""
    if min_ask_amount is not None:
        extra_conditions += " AND m.matched_amount >= :min_ask_amount"
        params["min_ask_amount"] = min_ask_amount
    if min_bid_amount is not None:
        extra_conditions += " AND m.paid_amount >= :min_bid_amount"
        params["min_bid_amount"] = min_bid_amount

    query = f"""
    WITH matches AS (
        SELECT
            "Order".id || '-' || "FullMatch".id AS id,
            "FullMatch".tx_hash AS txid,
            "FullMatch".matched_amount AS matched_amount,
            "FullMatch".paid_amount AS paid_amount,
            "FullMatch".slot_no AS slot_no,
            "Order".ask_token AS ask_token,
            "Order".bid_token AS bid_token,
            "Order".aggregator_platform AS aggregator_platform,
            "Order".dex_id AS dex_id
        FROM "Order" 
        INNER JOIN "FullMatch" ON "Order".full_match_id = "FullMatch".id
        UNION 
        SELECT 
            "Order".id || '-' || "PartialMatch".id AS id,
            SUBSTR("PartialMatch".new_utxo_id, 0, {substring_expr}) AS txid,
            "PartialMatch".matched_amount AS matched_amount,
            "PartialMatch".paid_amount AS paid_amount,
            "PartialMatch".slot_no AS slot_no,
            "Order".ask_token AS ask_token,
            "Order".bid_token AS bid_token,
            "Order".aggregator_platform AS aggregator_platform,
            "Order".dex_id AS dex_id
        FROM "PartialMatch" 
        JOIN "Order" ON "PartialMatch".order_id = "Order".id
    )
    
    SELECT * FROM matches m
    WHERE (
        (m.ask_token = :token_a AND m.bid_token = :token_b) OR
        (m.ask_token = :token_b AND m.bid_token = :token_a)
    )
    {dex_condition}
    {extra_conditions}
    ORDER BY slot_no DESC 
    LIMIT :limit OFFSET :offset
    """
    result = []

    with engine.connect() as con:
        sql_query = sqlalchemy.text(query)
        fields = ['tradeId', 'txHash', 'fulfilledAmount', 'bidAmount', 'matchedAt']
        for values in con.execute(sql_query, params):
            d = {k: v for k, v in zip(fields, values)}
            ask_token = values[5]
            quote_action = "buy" if ask_token == token_to else "sell"
            d['quoteAction'] = quote_action
            d['matchedAt'] = slot_timestamp(d['matchedAt'])
            result.append(d)
    return result


def get_orderbook(token_from: str, token_to: str, depth=50):
    """Returns orderbook a -> b *price-wise* i.e. already in the correct format"""
    # always order by how much people are willing to pay (bid / ask higher is shown first)
    query = """SELECT
    SUM("Order".bid_amount) as amount_from,
    SUM("Order".ask_amount) as amount_to,
    COUNT(*) as count,
    (cast("Order".bid_amount as float) / cast("Order".ask_amount as float)) as price
    from "Order"
     where
    ("Order".ask_token = :token_to and "Order".bid_token = :token_from) and
    "Order".dex_id = 'muesliswap' and
    "Order".ask_amount > 0 and
    "Order".full_match_id is null and "Order".cancellation_id is null
    group by price
    order by price desc 
    limit :depth
    """
    result = []
    params = {"token_to": token_to, "token_from": token_from, "depth": depth}

    with engine.connect() as con:
        sql_query = sqlalchemy.text(query)
        fields = ['toAmount', 'fromAmount']
        for values in con.execute(sql_query, params):
            d = {k: v for k, v in zip(fields, values)}
            d["providers"] = ["muesliswap_v4"]
            result.append(d)
    return result


def get_last_price(
    interval: int,
    to_timestamp: int,
    base_token: Optional[str] = None,
    quote_token: Optional[str] = None,
    dex_id: Optional[str] = None,
    aggregator_id: Optional[str] = None,
):
    """
    Find the mean price of the last interval that a token was traded (per default all tokens)
    Note: Omits doing cleanup for milk because there exist trades after the v2 launch

    """
    params = {
        "to_timestamp": to_timestamp,
    }

    token_condition = ''
    if base_token:
        token_condition += f' and m.ask_token = :token_a '
        params['token_a'] = base_token
    if quote_token:
        token_condition += f' and m.bid_token = :token_b '
        params['token_b'] = quote_token

    dex_condition = filter_by_dex(params, dex_id, aggregator_id)

    # Note this is just as expensive as querying the entire price chart
    query = f"""
    with matches as (SELECT
        "FullMatch".matched_amount as matched_amount,
        "FullMatch".paid_amount as paid_amount,
        "FullMatch".slot_no as slot_no,
        "Order".ask_token as ask_token,
        "Order".bid_token as bid_token,
        "Order".dex_id as dex_id,
        "Order".aggregator_platform as aggregator_platform
        from "Order" inner join "FullMatch" on "Order".full_match_id = "FullMatch".id
        UNION SELECT 
        "PartialMatch".matched_amount as matched_amount,
        "PartialMatch".paid_amount as paid_amount,
        "PartialMatch".slot_no as slot_no,
        "Order".ask_token as ask_token,
        "Order".bid_token as bid_token,
        "Order".dex_id as dex_id,
        "Order".aggregator_platform as aggregator_platform
        from "PartialMatch" join "Order" on "PartialMatch".order_id = "Order".id
        UNION SELECT  -- mirror all orders so the reverse appears in the same group
        "FullMatch".paid_amount as matched_amount,
        "FullMatch".matched_amount as paid_amount,
        "FullMatch".slot_no as slot_no,
        "Order".bid_token as ask_token,
        "Order".ask_token as bid_token,
        "Order".dex_id as dex_id,
        "Order".aggregator_platform as aggregator_platform
        from "Order" inner join "FullMatch" on "Order".full_match_id = "FullMatch".id
        UNION SELECT 
        "PartialMatch".paid_amount as matched_amount,
        "PartialMatch".matched_amount as paid_amount,
        "PartialMatch".slot_no as slot_no,
        "Order".bid_token as ask_token,
        "Order".ask_token as bid_token,
        "Order".dex_id as dex_id,
        "Order".aggregator_platform as aggregator_platform
        from "PartialMatch" join "Order" on "PartialMatch".order_id = "Order".id
    ),
    last_trade as (
        select max(slot_no) as slot_no, ask_token, bid_token
        from matches 
        where slot_no <= :to_timestamp
        group by ask_token, bid_token
    )

    select
        m.matched_amount,
        m.paid_amount,
        m.slot_no,
        m.ask_token,
        m.bid_token
    from matches m join last_trade lt on m.ask_token = lt.ask_token and m.bid_token = lt.bid_token
    where m.slot_no > lt.slot_no - :interval and m.slot_no <= lt.slot_no
    -- cleanup
    AND (m.ask_token != '' OR m.matched_amount >= 1000000)
    AND (m.bid_token != '' OR m.paid_amount >= 1000000)
    AND m.matched_amount > 0
    AND m.paid_amount > 0
    -- end cleanup
    {token_condition}
    {dex_condition}
    order by m.ask_token, m.bid_token, m.slot_no asc
    """
    params["interval"] = interval

    res_map = defaultdict(list)
    with engine.connect() as con:
        sql_query = sqlalchemy.text(query)
        cur_timestamp_cut = 0
        cur_amts = []
        cur_token_a, cur_token_b = None, None
        for values in itertools.chain(con.execute(sql_query, params), ((0, 0, 0, 0, 0),)):
            matched_amount, paid_amount, slot_no, ask_token, bid_token = values
            unix_timestamp = slot_timestamp(slot_no)
            unix_timestamp_cut = unix_timestamp // interval
            if cur_timestamp_cut != unix_timestamp_cut or cur_token_a != ask_token or cur_token_b != bid_token:
                if cur_amts:
                    cur_prices = [x[0] / x[1] for x in cur_amts]
                    frame = cur_timestamp_cut * interval
                    weights = [math.sqrt(x[0]*x[1]) for x in cur_amts]
                    cur_result = {
                        "datetime": datetime.datetime.utcfromtimestamp(frame).isoformat(),
                        "time": frame,
                        "start": frame,
                        "end": frame + interval,
                        "median": weighted_median(cur_prices, weights),
                        "mean": weighted_mean(cur_prices, weights),
                        "open": cur_prices[0],
                        "close": cur_prices[-1],
                        "high": max(cur_prices),
                        "low": min(cur_prices),
                        "volume_base": sum([x[0] for x in cur_amts]),
                        "volume_quote": sum([x[1] for x in cur_amts]),
                    }
                    res_map[(cur_token_a, cur_token_b)].append(cur_result)
                cur_timestamp_cut = unix_timestamp_cut
                cur_token_a, cur_token_b = ask_token, bid_token
                cur_amts = []
            cur_amts.append(
                (matched_amount, paid_amount)
            )
    return res_map


_all_token_stats_cache = {
    "last_price": None,
    "last_nine_days": None,
    "to_timestamp": None,
}


def get_all_token_stats(
    time_to: int=None,
    cache=_all_token_stats_cache
):
    _LOGGER.info("get_all_token_stats: Starting update")
    if time_to is None:
        time_to = int(datetime.datetime.now().timestamp())
    one_day_seconds = int(datetime.timedelta(days=1).total_seconds())
    today = int(time_to // one_day_seconds * one_day_seconds)
    ten_days_ago = today - 10 * one_day_seconds
    one_day_ago = today - 1 * one_day_seconds
    last_day_daily_token_stats = get_price_and_volume_chart(
        interval=one_day_seconds,
        from_timestamp=one_day_ago+1,
        to_timestamp=time_to,
    )
    _LOGGER.info("get_all_token_stats: Generated price and volume charts")

    if cache.get("to_timestamp") == one_day_ago:
        last_prices = cache["last_price"]
        last_nine_days_daily_token_stats = cache["last_nine_days"]
        _LOGGER.info("get_all_token_stats: Using cached last_prices and last_nine_days_daily_token_stats")
    else:
        # the last prices only include data until 10 days ago which is impossible to change
        # we can thus cache it
        last_prices = get_last_price(
            interval=one_day_seconds,
            to_timestamp=ten_days_ago,
        )
        _LOGGER.info("get_all_token_stats: Generated last_prices")

        # we split the daily stats into stats until 1 day ago and the 9 days before
        # the 9 days ago are highly unlikely to change and can thus be cached
        last_nine_days_daily_token_stats = get_price_and_volume_chart(
            interval=one_day_seconds,
            from_timestamp=ten_days_ago+1,
            to_timestamp=one_day_ago,
        )
        _LOGGER.info("get_all_token_stats: Generated last_nine_days_daily_token_stats")

        cache["to_timestamp"] = one_day_ago
        cache["last_price"] = last_prices
        cache["last_nine_days"] = last_nine_days_daily_token_stats

    res = []
    token_pairs = set(last_nine_days_daily_token_stats.keys()).union(last_prices.keys()).union(last_day_daily_token_stats.keys())
    for (from_token, to_token) in token_pairs:
        daily_stats = (
              last_nine_days_daily_token_stats.get((from_token, to_token), [])
              + last_day_daily_token_stats.get((from_token, to_token), [])
        )
        # check that latest day is actually today and that days are consecutive
        # (first day = 10 days ago and latest day = today) iff len(daily_stats) == 10
        daily_prices: List[float] = [x["mean"] for x in daily_stats[-10:]]
        if len(daily_stats) != 10:
            daily_prices_w_none = [None for _ in range(11)]
            for window in daily_stats:
                day_diff = (today - window["time"]) // int(one_day_seconds)
                daily_prices_w_none[10 - day_diff] = window["mean"]
            # finally calculate mean price for every day
            mean_prices = []
            # find the first price before the 10 day window or the last one within
            last_price = last_prices.get((from_token, to_token), [{"mean": 0}])[-1]["mean"]
            for day_price in daily_prices_w_none:
                mean_price = day_price if day_price is not None else last_price
                last_price = mean_price
                mean_prices.append(mean_price)
            daily_prices = mean_prices

        local_res = {}
        local_res["fromToken"] = from_token
        local_res["toToken"] = to_token
        local_res["priceChange"] = {
            "24h": daily_prices[-1] - daily_prices[-2],
            "7d": daily_prices[-1] - daily_prices[-8],
        }
        local_res["price10d"] = daily_prices
        if daily_stats and daily_stats[-1]["time"] >= one_day_ago:
            local_res["volume"] = {
                "base": daily_stats[-1]["volume_base"],
                "quote": daily_stats[-1]["volume_quote"],
            }
        else:
            local_res["volume"] = {
                "base": 0,
                "quote": 0,
            }
        res.append(local_res)

    _LOGGER.info("get_all_token_stats: Processed %d token pairs" % len(token_pairs))
    _LOGGER.info("get_all_token_stats: Data generated successfully")
    return res
