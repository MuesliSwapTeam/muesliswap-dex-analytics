import logging
from datetime import datetime, timedelta

from flask import Flask, request, abort, jsonify, send_file
from flask_cors import CORS
from flask_caching import Cache
from flask_restx import Api, Resource, fields

from . import (
    A_POLICYID_ARG,
    A_TOKENNAME_ARG,
    B_POLICYID_ARG,
    B_TOKENNAME_ARG,
    TOKEN_SUBJECT_ARG,
    FROM_ARG,
    CACHE_DEFAULT_TIMEOUT,
    SENDER_PKH_ARG,
    SENDER_SKH_ARG,
    LIMIT_ARG,
    OFFSET_ARG,
    DATA_DIR,
)

from . import util

# Logger setup
_LOGGER = logging.getLogger(__name__)

# Flask app setup
app = Flask(__name__)
app.config.from_mapping(
    {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": CACHE_DEFAULT_TIMEOUT}
)
cache = Cache(app)
CORS(app)

# Flask-RESTx API setup
api = Api(app, doc="/docs", title="DEX Analytics API Documentation", version="1.0", description="An API documentation for the MuesliSwap DEX analytics Catalyst project.")

# Namespaces
ns_orders = api.namespace('orders', description="Order-related operations")
ns_volume = api.namespace('volume', description="Volume-related operations")
ns_chart = api.namespace('charts', description="Chart-related operations")
ns_stats = api.namespace('stats', description="Statistics-related operations")
ns_trades = api.namespace('trades', description="Trade-related operations")

#################################################################################################
#                                            Models                                             #
#################################################################################################

volume_response_model = api.model(
    "VolumeResponse",
    {
        "token_a": fields.String(description="Token A"),
        "token_b": fields.String(description="Token B"),
        "volume": fields.Float(description="Trading volume"),
    },
)

trade_response_model = api.model(
    "TradeResponse",
    {
        "token_from": fields.String(description="Source token for the trade"),
        "token_to": fields.String(description="Target token for the trade"),
        "amount": fields.Float(description="Trade amount"),
        "timestamp": fields.DateTime(description="Trade timestamp"),
    },
)

orderbook_response_model = api.model(
    "OrderBookResponse",
    {
        "orders": fields.List(fields.Raw, description="List of orders in the orderbook"),
    },
)


#################################################################################################
#                                    Helper Functions                                           #
#################################################################################################

def get_volume_sliding_window(policyid_a, tokenname_a, policyid_b, tokenname_b, aggregator_id, time_from):
    try:
        stats = util.get_current_volume(
            tokenname_a=tokenname_a,
            policy_id_a=policyid_a,
            tokenname_b=tokenname_b,
            policy_id_b=policyid_b,
            aggregator_id=aggregator_id,
            time_from=time_from,
        )
        return jsonify(stats)
    except Exception as e:
        _LOGGER.error("failed to query volume", exc_info=e)
        abort(500)


#################################################################################################
#                                    Volume Endpoints                                           #
#################################################################################################

@ns_volume.route("/current/")
class CurrentVolume(Resource):
    @cache.cached(make_cache_key=lambda: (request.path, request.args))
    @ns_volume.doc(
        params={
            FROM_ARG: "Start timestamp for the volume calculation.",
            "aggregator-id": "Aggregator ID for filtering results (optional).",
            A_POLICYID_ARG: "Policy ID of token A (optional).",
            B_POLICYID_ARG: "Policy ID of token B (optional).",
            A_TOKENNAME_ARG: "Token name of token A (optional).",
            B_TOKENNAME_ARG: "Token name of token B (optional).",
        },
        responses={
            200: ("Success", volume_response_model),
            400: "Bad request",
            500: "Internal server error",
        },
    )
    def get(self):
        """
        Get the current trading volume within a sliding time window.

        Returns:
            JSON: Trading volume statistics.
        """
        time_from = request.args.get(FROM_ARG, type=int)
        if time_from is None:
            abort(400, f'please provide the "{FROM_ARG}" argument')
        aggregator_id = request.args.get("aggregator-id", type=str, default=None)
        return get_volume_sliding_window(
            policyid_a=request.args.get(A_POLICYID_ARG),
            policyid_b=request.args.get(B_POLICYID_ARG),
            tokenname_a=request.args.get(A_TOKENNAME_ARG),
            tokenname_b=request.args.get(B_TOKENNAME_ARG),
            aggregator_id=aggregator_id,
            time_from=time_from,
        )

@ns_volume.route("/daily/")
class DailyVolume(Resource):
    def get(self):
        """
        Get the daily trading volume.

        Returns:
            JSON: Daily trading volume statistics.
        """
        aggregator_id = request.args.get("aggregator-id", type=str, default=None)
        return get_volume_sliding_window(
            policyid_a=request.args.get(A_POLICYID_ARG),
            policyid_b=request.args.get(B_POLICYID_ARG),
            tokenname_a=request.args.get(A_TOKENNAME_ARG),
            tokenname_b=request.args.get(B_TOKENNAME_ARG),
            aggregator_id=aggregator_id,
            time_from=(datetime.now() - timedelta(days=1)).timestamp(),
        )

@ns_volume.route("/weekly/")
class WeeklyVolume(Resource):
    def get(self):
        """
        Get the weekly trading volume.

        Returns:
            JSON: Weekly trading volume statistics.
        """
        aggregator_id = request.args.get("aggregator-id", type=str, default=None)
        return get_volume_sliding_window(
            policyid_a=request.args.get(A_POLICYID_ARG),
            policyid_b=request.args.get(B_POLICYID_ARG),
            tokenname_a=request.args.get(A_TOKENNAME_ARG),
            tokenname_b=request.args.get(B_TOKENNAME_ARG),
            aggregator_id=aggregator_id,
            time_from=(datetime.now() - timedelta(days=7)).timestamp(),
        )


#################################################################################################
#                                    Order-Related Endpoints                                    #
#################################################################################################

@ns_orders.route("/history/")
class OrderHistory(Resource):
    @cache.cached(make_cache_key=lambda: (request.path, request.args))
    @ns_orders.doc(
        params={
            SENDER_PKH_ARG: "Public key hash of the sender's wallet.",
            SENDER_SKH_ARG: "Secondary key hash of the sender's wallet.",
            "aggregator-id": "Aggregator ID for filtering results (optional).",
            LIMIT_ARG: "Maximum number of results to return (default: 100).",
            OFFSET_ARG: "Offset for pagination (default: 0).",
        },
    )
    def get(self):
        """
        Get the order history for a specific wallet.

        Returns:
            JSON: List of historical orders.
        """
        sender_pkh = request.args.get(SENDER_PKH_ARG)
        sender_skh = request.args.get(SENDER_SKH_ARG)
        limit = request.args.get(LIMIT_ARG, default=100)
        offset = request.args.get(OFFSET_ARG, default=0)
        if sender_pkh == '*':
            sender_pkh = None
        if sender_skh == '*':
            sender_skh = None
        if not sender_pkh and not sender_skh:
            abort(400, f"Please provide at least one of: {SENDER_PKH_ARG}, {SENDER_SKH_ARG}.")
        aggregator_id = request.args.get("aggregator-id", type=str, default=None)
        return jsonify(util.get_order_history(sender_pkh, sender_skh, aggregator_id=aggregator_id, limit=limit, offset=offset))


#################################################################################################
#                                    Chart-Related Endpoints                                    #
#################################################################################################

@ns_chart.route("/price/")
class PriceChart(Resource):
    @cache.cached(make_cache_key=lambda: (request.path, request.args))
    @ns_chart.doc(
        params={
            "base-token": "Base token for the chart.",
            "quote-token": "Quote token for the chart.",
            "dex-id": "DEX ID for filtering data (optional).",
            "aggregator-id": "Aggregator ID for filtering data (optional).",
            "from-timestamp": "Start timestamp for the chart data (optional).",
            "to-timestamp": "End timestamp for the chart data (optional).",
            "interval": "Time interval for the chart data (default: 1 day).",
        },
    )
    def get(self):
        """
        Get price chart data for a token pair.

        Returns:
            JSON: Price chart data for the token pair.
        """
        token_base = request.args.get("base-token", type=str)
        token_quote = request.args.get("quote-token", type=str)
        interval = request.args.get("interval", type=int, default=86400)
        if token_base is None or token_quote is None or token_base == token_quote:
            abort(400, "Invalid tokens provided.")
        return jsonify(util.get_token_price_and_volume_chart(
            base_token=token_base,
            quote_token=token_quote,
            interval=interval,
        ))


#################################################################################################
#                                    Trade-Related Endpoints                                    #
#################################################################################################

@ns_trades.route("/")
class Trades(Resource):
    @cache.cached(make_cache_key=lambda: (request.path, request.args))
    @ns_trades.doc(
        params={
            "from-token": "Source token for trades.",
            "to-token": "Target token for trades.",
            "limit": "Maximum number of results to return (default: 100).",
            "offset": "Offset for pagination (default: 0).",
            "aggregator-id": "Aggregator ID for filtering results (optional).",
        },
        responses={200: ("Success", trade_response_model)},
    )
    def get(self):
        """
        Get recent trades between two tokens.

        Returns:
            JSON: List of recent trades.
        """
        token_from = request.args.get("from-token", type=str)
        token_to = request.args.get("to-token", type=str)
        limit = request.args.get("limit", type=int, default=100)
        offset = request.args.get("offset", type=int, default=0)
        if token_from is None or token_to is None or token_from == token_to:
            abort(400, "Invalid tokens provided.")
        return jsonify(util.get_trades(token_from, token_to, limit, offset))