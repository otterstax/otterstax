# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax
"""
Shared SQL definitions for all benchmark tests.
All three frontends (mysql, postgres, arrow) use the same SQL strings.
Only the connector library and port change per frontend.

Data layout (controlled by bench.yaml):
  Group A (mysql1 / pg1 / ch1) — BIG: ~60k impressions, ~60k daily_stats
  Group B (mysql2 / pg2 / ch2) — SMALL: ~5k products, ~5k orders, ~4k events
"""

SIMPLE_SELECT = [
    ("group_a_mysql1",
     "SELECT campaign_id, campaign_name, budget, status"
     " FROM mysql1.benchdb1.schema.campaigns"
     " WHERE status = 'active' LIMIT 100"),
    ("group_a_pg1",
     "SELECT campaign_id, campaign_name, budget, status"
     " FROM pg1.benchpg1.public.campaigns"
     " WHERE status = 'active' LIMIT 100"),
    ("group_a_ch1",
     "SELECT campaign_id, campaign_name, budget, status"
     " FROM ch1.benchch1.schema.campaigns"
     " WHERE status = 'active' LIMIT 100"),
    ("group_b_mysql2",
     "SELECT product_id, product_name, category, price"
     " FROM mysql2.benchdb2.schema.products"
     " WHERE price > 50 LIMIT 100"),
    ("group_b_pg2",
     "SELECT product_id, product_name, category, price"
     " FROM pg2.benchpg2.public.products"
     " WHERE price > 50 LIMIT 100"),
    ("group_b_ch2",
     "SELECT product_id, product_name, category, price"
     " FROM ch2.benchch2.schema.products"
     " WHERE price > 50 LIMIT 100"),
]

COMPLEX_SELECT = [
    # Group A — impressions (~60k rows): GROUP BY campaign → ~1000 result rows
    ("impressions_mysql1",
     "SELECT campaign_id, SUM(clicks) AS total_clicks,"
     " SUM(views) AS total_views, AVG(cost) AS avg_cost, COUNT(*) AS days"
     " FROM mysql1.benchdb1.schema.impressions"
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
     " FROM ch1.benchch1.schema.impressions"
     " WHERE clicks > 10 AND cost > 1.0"
     " GROUP BY campaign_id HAVING SUM(clicks) > 100"
     " ORDER BY total_clicks DESC LIMIT 1000"),
    # Group B — orders (~5k rows, ~5 per campaign): GROUP BY campaign → ~1000 rows
    ("orders_mysql2",
     "SELECT campaign_id, COUNT(*) AS order_count,"
     " SUM(total_price) AS revenue, AVG(unit_price) AS avg_price,"
     " MAX(quantity) AS max_qty"
     " FROM mysql2.benchdb2.schema.orders"
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
     " FROM ch2.benchch2.schema.orders"
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
     " FROM mysql1.benchdb1.schema.campaigns c"
     " JOIN mysql1.benchdb1.schema.impressions i"
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
     " FROM ch1.benchch1.schema.campaigns c"
     " JOIN ch1.benchch1.schema.impressions i"
     " ON c.campaign_id = i.campaign_id"
     " WHERE c.campaign_id <= 67"),
    ("prod_ord_mysql2",
     "SELECT p.product_id, p.product_name, p.category,"
     " o.quantity, o.total_price"
     " FROM mysql2.benchdb2.schema.products p"
     " JOIN mysql2.benchdb2.schema.orders o"
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
     " FROM ch2.benchch2.schema.products p"
     " JOIN ch2.benchch2.schema.orders o"
     " ON p.product_id = o.product_id"),
]

# ── JOIN_CROSS_ENGINE ──────────────────────────────────────────────────────────
# Joins Group B tables across different engine instances (all small ~5k rows).
# "SELECT * FROM little_mysql2 JOIN little_pg2 / little_ch2"
JOIN_CROSS_ENGINE = [
    ("prod_mysql2_x_ord_pg2",
     "SELECT p.product_id, p.product_name, p.category,"
     " o.quantity, o.total_price"
     " FROM mysql2.benchdb2.schema.products p"
     " JOIN pg2.benchpg2.public.orders o"
     " ON p.product_id = o.product_id"),
    ("prod_mysql2_x_ord_ch2",
     "SELECT p.product_id, p.product_name, p.category,"
     " o.quantity, o.total_price"
     " FROM mysql2.benchdb2.schema.products p"
     " JOIN ch2.benchch2.schema.orders o"
     " ON p.product_id = o.product_id"),
    ("prod_pg2_x_ord_mysql2",
     "SELECT p.product_id, p.product_name, p.category,"
     " o.quantity, o.total_price"
     " FROM pg2.benchpg2.public.products p"
     " JOIN mysql2.benchdb2.schema.orders o"
     " ON p.product_id = o.product_id"),
    ("prod_pg2_x_ord_ch2",
     "SELECT p.product_id, p.product_name, p.category,"
     " o.quantity, o.total_price"
     " FROM pg2.benchpg2.public.products p"
     " JOIN ch2.benchch2.schema.orders o"
     " ON p.product_id = o.product_id"),
    ("prod_ch2_x_ord_mysql2",
     "SELECT p.product_id, p.product_name, p.category,"
     " o.quantity, o.total_price"
     " FROM ch2.benchch2.schema.products p"
     " JOIN mysql2.benchdb2.schema.orders o"
     " ON p.product_id = o.product_id"),
    ("prod_ch2_x_ord_pg2",
     "SELECT p.product_id, p.product_name, p.category,"
     " o.quantity, o.total_price"
     " FROM ch2.benchch2.schema.products p"
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
     " FROM mysql1.benchdb1.schema.campaigns c"
     " JOIN pg1.benchpg1.public.impressions i"
     "   ON c.campaign_id = i.campaign_id AND i.clicks > 500"
     " JOIN ch2.benchch2.schema.orders o"
     "   ON c.campaign_id = o.campaign_id AND o.total_price > 500.0"
     " WHERE c.campaign_id <= 50 AND c.status = 'active'"
     " LIMIT 500"),
    ("variant2_pg_ch_mysql",
     "SELECT c.campaign_id, c.campaign_name,"
     " d.total_revenue,"
     " e.event_type, e.device"
     " FROM pg1.benchpg1.public.campaigns c"
     " JOIN ch1.benchch1.schema.daily_stats d"
     "   ON c.campaign_id = d.campaign_id AND d.total_revenue > 5000.0"
     " JOIN mysql2.benchdb2.schema.events e"
     "   ON c.campaign_id = e.campaign_id AND e.event_type = 'purchase'"
     " WHERE c.campaign_id <= 50 AND c.status = 'active'"
     " LIMIT 500"),
]
