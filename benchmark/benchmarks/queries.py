# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax
"""
Shared SQL definitions for all benchmark tests.
Both frontends (mysql, postgres) use the same SQL strings.
Only the connector library and port change per frontend.

Data layout (controlled by bench.yaml):
  Group A (mysql1 / pg1 / ch1) — BIG: ~60k impressions, ~60k daily_stats
  Group B (mysql2 / pg2 / ch2) — SMALL: ~5k products, ~5k orders, ~4k events
"""

SIMPLE_SELECT = [
    ("group_a_mysql1",
     "SELECT campaign_id, campaign_name, budget, status"
     " FROM mysql1.benchdb1.campaigns"
     " WHERE status = 'active' LIMIT 100"),
    ("group_a_pg1",
     "SELECT campaign_id, campaign_name, budget, status"
     " FROM pg1.benchpg1.public.campaigns"
     " WHERE status = 'active' LIMIT 100"),
    ("group_a_ch1",
     "SELECT campaign_id, campaign_name, budget, status"
     " FROM ch1.benchch1.campaigns"
     " WHERE status = 'active' LIMIT 100"),
    ("group_b_mysql2",
     "SELECT product_id, product_name, category, price"
     " FROM mysql2.benchdb2.products"
     " WHERE price > 50 LIMIT 100"),
    ("group_b_pg2",
     "SELECT product_id, product_name, category, price"
     " FROM pg2.benchpg2.public.products"
     " WHERE price > 50 LIMIT 100"),
    ("group_b_ch2",
     "SELECT product_id, product_name, category, price"
     " FROM ch2.benchch2.products"
     " WHERE price > 50 LIMIT 100"),
]

COMPLEX_SELECT = [
    # Group A — impressions (~60k rows): GROUP BY campaign → ~1000 result rows
    ("impressions_mysql1",
     "SELECT campaign_id, SUM(clicks) AS total_clicks,"
     " SUM(views) AS total_views, AVG(cost) AS avg_cost, COUNT(*) AS days"
     " FROM mysql1.benchdb1.impressions"
     " WHERE clicks > 10 AND cost > 1.0"
     " GROUP BY campaign_id HAVING total_clicks > 100"
     " ORDER BY total_clicks DESC LIMIT 1000"),
    ("impressions_pg1",
     "SELECT campaign_id, SUM(clicks) AS total_clicks,"
     " SUM(views) AS total_views, AVG(cost) AS avg_cost, COUNT(*) AS days"
     " FROM pg1.benchpg1.public.impressions"
     " WHERE clicks > 10 AND cost > 1.0"
     " GROUP BY campaign_id HAVING SUM(clicks) > 100"
     " ORDER BY total_clicks DESC LIMIT 1000"),
    ("impressions_ch1",
     "SELECT campaign_id, SUM(clicks) AS total_clicks,"
     " SUM(views) AS total_views, AVG(cost) AS avg_cost, COUNT(*) AS days"
     " FROM ch1.benchch1.impressions"
     " WHERE clicks > 10 AND cost > 1.0"
     " GROUP BY campaign_id HAVING SUM(clicks) > 100"
     " ORDER BY total_clicks DESC LIMIT 1000"),
    # Group B — orders (~5k rows, ~5 per campaign): GROUP BY campaign → ~1000 rows
    ("orders_mysql2",
     "SELECT campaign_id, COUNT(*) AS order_count,"
     " SUM(total_price) AS revenue, AVG(unit_price) AS avg_price,"
     " MAX(quantity) AS max_qty"
     " FROM mysql2.benchdb2.orders"
     " WHERE total_price > 10.0 AND quantity > 1"
     " GROUP BY campaign_id HAVING order_count > 2"
     " ORDER BY revenue DESC LIMIT 1000"),
    ("orders_pg2",
     "SELECT campaign_id, COUNT(*) AS order_count,"
     " SUM(total_price) AS revenue, AVG(unit_price) AS avg_price,"
     " MAX(quantity) AS max_qty"
     " FROM pg2.benchpg2.public.orders"
     " WHERE total_price > 10.0 AND quantity > 1"
     " GROUP BY campaign_id HAVING COUNT(*) > 2"
     " ORDER BY revenue DESC LIMIT 1000"),
    ("orders_ch2",
     "SELECT campaign_id, COUNT(*) AS order_count,"
     " SUM(total_price) AS revenue, AVG(unit_price) AS avg_price,"
     " MAX(quantity) AS max_qty"
     " FROM ch2.benchch2.orders"
     " WHERE total_price > 10.0 AND quantity > 1"
     " GROUP BY campaign_id HAVING COUNT(*) > 2"
     " ORDER BY revenue DESC LIMIT 1000"),
]

# ── JOIN_SAME_INSTANCE ─────────────────────────────────────────────────────────
# Joins two tables within the same engine instance.
# Group A (camp × imp): big tables — WHERE filter limits to ~4 000 rows
# Group B (prod × ord): small tables — naturally ~5 000 rows
JOIN_SAME_INSTANCE = [
    ("camp_imp_mysql1",
     "SELECT c.campaign_id, c.campaign_name, c.budget,"
     " i.views, i.clicks, i.cost"
     " FROM mysql1.benchdb1.campaigns c"
     " JOIN mysql1.benchdb1.impressions i"
     " ON c.campaign_id = i.campaign_id"
     " WHERE c.campaign_id <= 67"),        # 67 campaigns × 60 imp = ~4 020 rows
    ("camp_imp_pg1",
     "SELECT c.campaign_id, c.campaign_name, c.budget,"
     " i.views, i.clicks, i.cost"
     " FROM pg1.benchpg1.public.campaigns c"
     " JOIN pg1.benchpg1.public.impressions i"
     " ON c.campaign_id = i.campaign_id"
     " WHERE c.campaign_id <= 67"),
    ("camp_imp_ch1",
     "SELECT c.campaign_id, c.campaign_name, c.budget,"
     " i.views, i.clicks, i.cost"
     " FROM ch1.benchch1.campaigns c"
     " JOIN ch1.benchch1.impressions i"
     " ON c.campaign_id = i.campaign_id"
     " WHERE c.campaign_id <= 67"),
    ("prod_ord_mysql2",
     "SELECT p.product_id, p.product_name, p.category,"
     " o.quantity, o.total_price"
     " FROM mysql2.benchdb2.products p"
     " JOIN mysql2.benchdb2.orders o"
     " ON p.product_id = o.product_id"),   # ~5 000 rows (small Group B)
    ("prod_ord_pg2",
     "SELECT p.product_id, p.product_name, p.category,"
     " o.quantity, o.total_price"
     " FROM pg2.benchpg2.public.products p"
     " JOIN pg2.benchpg2.public.orders o"
     " ON p.product_id = o.product_id"),
    ("prod_ord_ch2",
     "SELECT p.product_id, p.product_name, p.category,"
     " o.quantity, o.total_price"
     " FROM ch2.benchch2.products p"
     " JOIN ch2.benchch2.orders o"
     " ON p.product_id = o.product_id"),
]

# ── JOIN_CROSS_ENGINE ──────────────────────────────────────────────────────────
# Joins Group B tables across different engine instances (all small ~5k rows).
# "SELECT * FROM little_mysql2 JOIN little_pg2 / little_ch2"
JOIN_CROSS_ENGINE = [
    ("prod_mysql2_x_ord_pg2",
     "SELECT p.product_id, p.product_name, p.category,"
     " o.quantity, o.total_price"
     " FROM mysql2.benchdb2.products p"
     " JOIN pg2.benchpg2.public.orders o"
     " ON p.product_id = o.product_id"),
    ("prod_mysql2_x_ord_ch2",
     "SELECT p.product_id, p.product_name, p.category,"
     " o.quantity, o.total_price"
     " FROM mysql2.benchdb2.products p"
     " JOIN ch2.benchch2.orders o"
     " ON p.product_id = o.product_id"),
    ("prod_pg2_x_ord_mysql2",
     "SELECT p.product_id, p.product_name, p.category,"
     " o.quantity, o.total_price"
     " FROM pg2.benchpg2.public.products p"
     " JOIN mysql2.benchdb2.orders o"
     " ON p.product_id = o.product_id"),
    ("prod_pg2_x_ord_ch2",
     "SELECT p.product_id, p.product_name, p.category,"
     " o.quantity, o.total_price"
     " FROM pg2.benchpg2.public.products p"
     " JOIN ch2.benchch2.orders o"
     " ON p.product_id = o.product_id"),
    ("prod_ch2_x_ord_mysql2",
     "SELECT p.product_id, p.product_name, p.category,"
     " o.quantity, o.total_price"
     " FROM ch2.benchch2.products p"
     " JOIN mysql2.benchdb2.orders o"
     " ON p.product_id = o.product_id"),
    ("prod_ch2_x_ord_pg2",
     "SELECT p.product_id, p.product_name, p.category,"
     " o.quantity, o.total_price"
     " FROM ch2.benchch2.products p"
     " JOIN pg2.benchpg2.public.orders o"
     " ON p.product_id = o.product_id"),
]

# ── JOIN_ALL ───────────────────────────────────────────────────────────────────
# Three-engine joins.  campaign_id <= 50 keeps the driving set tiny.
JOIN_ALL = [
    ("variant1_mysql_pg_ch",
     "SELECT c.campaign_id, c.campaign_name,"
     " i.clicks, i.cost,"
     " o.order_id, o.total_price"
     " FROM mysql1.benchdb1.campaigns c"
     " JOIN pg1.benchpg1.public.impressions i"
     "   ON c.campaign_id = i.campaign_id AND i.clicks > 500"
     " JOIN ch2.benchch2.orders o"
     "   ON c.campaign_id = o.campaign_id AND o.total_price > 500.0"
     " WHERE c.campaign_id <= 50 AND c.status = 'active'"
     " LIMIT 500"),
    ("variant2_pg_ch_mysql",
     "SELECT c.campaign_id, c.campaign_name,"
     " d.total_revenue,"
     " e.event_type, e.device"
     " FROM pg1.benchpg1.public.campaigns c"
     " JOIN ch1.benchch1.daily_stats d"
     "   ON c.campaign_id = d.campaign_id AND d.total_revenue > 5000.0"
     " JOIN mysql2.benchdb2.events e"
     "   ON c.campaign_id = e.campaign_id AND e.event_type = 'purchase'"
     " WHERE c.campaign_id <= 50 AND c.status = 'active'"
     " LIMIT 500"),
]

# ── Per-backend suite (benchmarks/backends/suite.py) ──────────────────────────
# The same four tests for every backend: <backend>_select_wide, _insert, _prepare,
# _concurrent. Each backend's entry names the same shapes on its own tables.
#   wide:       ~60k-row results (Group A: 1000 campaigns × 60) — decoding and the
#               frontend's row streaming dominate, not the backend;
#   insert:     the INSERT target, created empty by data/init_data.py before
#               OtterStax starts, so startup discovery registers it;
#   statements: a backend-heavy statement (GROUP BY over ~60k rows) and a light
#               one (100 rows), so _prepare and _concurrent separate backend time
#               from OtterStax's own.
def _wide(table_prefix):
    return [
        ("impressions_{}_all",
         "SELECT impression_id, campaign_id, views, clicks, cost"
         f" FROM {table_prefix}.impressions"),
        ("daily_stats_{}_all",
         "SELECT stat_id, campaign_id, total_spend, total_revenue,"
         " conversion_count, ctr, roas"
         f" FROM {table_prefix}.daily_stats"),
    ]


BACKEND_SUITES = {
    "ch": {
        "alias": "ch1",
        "wide": _wide("ch1.benchch1"),
        "insert": "ch2.benchch2.bench_inserts",
        "statements": [
            ("agg_ch1", dict(COMPLEX_SELECT)["impressions_ch1"]),
            ("select_ch2", dict(SIMPLE_SELECT)["group_b_ch2"]),
        ],
    },
    "mysql": {
        "alias": "mysql1",
        "wide": _wide("mysql1.benchdb1"),
        "insert": "mysql2.benchdb2.bench_inserts",
        "statements": [
            ("agg_mysql1", dict(COMPLEX_SELECT)["impressions_mysql1"]),
            ("select_mysql2", dict(SIMPLE_SELECT)["group_b_mysql2"]),
        ],
    },
    "pg": {
        "alias": "pg1",
        "wide": _wide("pg1.benchpg1.public"),
        "insert": "pg2.benchpg2.public.bench_inserts",
        "statements": [
            ("agg_pg1", dict(COMPLEX_SELECT)["impressions_pg1"]),
            ("select_pg2", dict(SIMPLE_SELECT)["group_b_pg2"]),
        ],
    },
}
for _suite in BACKEND_SUITES.values():
    _suite["wide"] = [(name.format(_suite["alias"]), sql) for name, sql in _suite["wide"]]

# Kept for the names the ClickHouse suite has always had.
CH_WIDE_SELECT = BACKEND_SUITES["ch"]["wide"]
CH_INSERT_TABLE = BACKEND_SUITES["ch"]["insert"]
CH_STATEMENTS = BACKEND_SUITES["ch"]["statements"]

# join_concurrent: cross-backend JOINs from N sessions at once — a light two-engine
# join, a ClickHouse one, and a three-engine join.
JOIN_CONCURRENT = [
    ("mysql2_x_pg2", dict(JOIN_CROSS_ENGINE)["prod_mysql2_x_ord_pg2"]),
    ("ch2_x_mysql2", dict(JOIN_CROSS_ENGINE)["prod_ch2_x_ord_mysql2"]),
    ("pg1_ch1_mysql2", dict(JOIN_ALL)["variant2_pg_ch_mysql"]),
]
