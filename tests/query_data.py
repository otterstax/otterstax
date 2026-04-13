#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""
Script for querying and displaying data from MariaDB, PostgreSQL, and ClickHouse instances.

Data Model:
- MariaDB1: campaigns (campaign_id, campaign_name, campaign_length, budget)
- MariaDB2: impressions (impression_id, campaign_id, days_since_start, clicks, conversions, revenue)
- PostgreSQL: products (product_id, campaign_id, product_name, price, category)
- ClickHouse: orders (order_id, campaign_id, product_id, customer_name, order_date, quantity, total_amount)

Cross-database correlation: campaign_id links all tables, product_id links products↔orders
"""
import pymysql
import psycopg2
import clickhouse_driver
from tabulate import tabulate

# Connection parameters for the first MariaDB server (campaigns)
CAMPAIGNS_DB_HOST = "mariadb1"
CAMPAIGNS_DB_PORT = 3306
CAMPAIGNS_DB_USER = "user1"
CAMPAIGNS_DB_PASSWORD = "password1"
CAMPAIGNS_DB_NAME = "db1"

# Connection parameters for the second MariaDB server (impressions)
IMPRESSIONS_DB_HOST = "mariadb2"
IMPRESSIONS_DB_PORT = 3306
IMPRESSIONS_DB_USER = "user2"
IMPRESSIONS_DB_PASSWORD = "password2"
IMPRESSIONS_DB_NAME = "db2"

# Connection parameters for PostgreSQL server (products)
POSTGRES_DB_HOST = "postgres1"
POSTGRES_DB_PORT = 5432
POSTGRES_DB_USER = "pguser"
POSTGRES_DB_PASSWORD = "pgpassword"
POSTGRES_DB_NAME = "pgdb"

# Connection parameters for ClickHouse server (orders)
CLICKHOUSE_DB_HOST = "clickhouse1"
CLICKHOUSE_DB_PORT = 9000
CLICKHOUSE_DB_USER = "chuser"
CLICKHOUSE_DB_PASSWORD = "chpassword"
CLICKHOUSE_DB_NAME = "chdb"


def get_campaigns():
    """Retrieves campaign data from the first MariaDB server."""
    conn = pymysql.connect(
        host=CAMPAIGNS_DB_HOST,
        port=CAMPAIGNS_DB_PORT,
        user=CAMPAIGNS_DB_USER,
        password=CAMPAIGNS_DB_PASSWORD,
        database=CAMPAIGNS_DB_NAME
    )

    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute("SELECT * FROM campaigns")
            return cursor.fetchall()
    finally:
        conn.close()


def get_impressions():
    """Retrieves impression data from the second MariaDB server."""
    conn = pymysql.connect(
        host=IMPRESSIONS_DB_HOST,
        port=IMPRESSIONS_DB_PORT,
        user=IMPRESSIONS_DB_USER,
        password=IMPRESSIONS_DB_PASSWORD,
        database=IMPRESSIONS_DB_NAME
    )

    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute("SELECT * FROM impressions")
            return cursor.fetchall()
    finally:
        conn.close()


def get_products():
    """Retrieves product data from PostgreSQL server."""
    conn = psycopg2.connect(
        host=POSTGRES_DB_HOST,
        port=POSTGRES_DB_PORT,
        user=POSTGRES_DB_USER,
        password=POSTGRES_DB_PASSWORD,
        database=POSTGRES_DB_NAME
    )

    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM products")
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
    finally:
        conn.close()


def get_orders():
    """Retrieves order data from ClickHouse server."""
    conn = clickhouse_driver.Client(
        host=CLICKHOUSE_DB_HOST,
        port=CLICKHOUSE_DB_PORT,
        user=CLICKHOUSE_DB_USER,
        password=CLICKHOUSE_DB_PASSWORD,
        database=CLICKHOUSE_DB_NAME
    )

    try:
        cursor = conn.execute("SELECT * FROM orders LIMIT 100")
        columns = ['order_id', 'campaign_id', 'product_id', 'customer_name', 'order_date', 'quantity', 'total_amount']
        rows = cursor
        return [dict(zip(columns, row)) for row in rows]
    finally:
        conn.disconnect()


def verify_data_correlation():
    """Verifies that data is properly correlated across databases."""
    print("\n" + "="*60)
    print("🔍 Verifying Cross-Database Data Correlation")
    print("="*60)
    
    try:
        # Get campaign IDs from MariaDB
        campaigns = get_campaigns()
        campaign_ids = set(c['campaign_id'] for c in campaigns)
        print(f"✓ MariaDB: {len(campaign_ids)} campaigns (IDs: {min(campaign_ids)}-{max(campaign_ids)})")
        
        # Get impression campaign IDs
        impressions = get_impressions()
        imp_campaign_ids = set(i['campaign_id'] for i in impressions)
        print(f"✓ MariaDB: {len(impressions)} impressions across {len(imp_campaign_ids)} campaigns")
        
        # Check campaign ID match
        missing_in_impressions = campaign_ids - imp_campaign_ids
        if missing_in_impressions:
            print(f"  ⚠️  Campaigns without impressions: {missing_in_impressions}")
        else:
            print(f"  ✅ All campaigns have impressions")
        
        # Get product campaign IDs from PostgreSQL
        try:
            products = get_products()
            if products:
                prod_campaign_ids = set(p['campaign_id'] for p in products)
                print(f"✓ PostgreSQL: {len(products)} products across {len(prod_campaign_ids)} campaigns")
                
                missing_in_products = campaign_ids - prod_campaign_ids
                if missing_in_products:
                    print(f"  ⚠️  Campaigns without products: {missing_in_products}")
                else:
                    print(f"  ✅ All campaigns have products")
        except Exception as e:
            print(f"⚠️  Could not verify PostgreSQL: {e}")
            products = None
        
        # Get order campaign IDs from ClickHouse
        try:
            orders = get_orders()
            if orders:
                order_campaign_ids = set(o['campaign_id'] for o in orders)
                print(f"✓ ClickHouse: {len(orders)} orders across {len(order_campaign_ids)} campaigns")
                
                missing_in_orders = campaign_ids - order_campaign_ids
                if missing_in_orders:
                    print(f"  ⚠️  Campaigns without orders: {missing_in_orders}")
                else:
                    print(f"  ✅ All campaigns have orders")
                
                # Verify product_id correlation
                if products:
                    product_ids = set(p['product_id'] for p in products)
                    order_product_ids = set(o['product_id'] for o in orders)
                    
                    invalid_product_refs = order_product_ids - product_ids
                    if invalid_product_refs:
                        print(f"  ⚠️  Orders reference {len(invalid_product_refs)} non-existent product IDs")
                        if len(invalid_product_refs) <= 10:
                            print(f"     Invalid product IDs: {invalid_product_refs}")
                    else:
                        print(f"  ✅ All order product_ids reference valid products")
        except Exception as e:
            print(f"⚠️  Could not verify ClickHouse: {e}")
            orders = None
            
    except Exception as e:
        print(f"❌ Verification failed: {e}")


def display_campaigns(campaigns):
    """Displays campaign data in table format."""
    if not campaigns:
        print("No campaign data available.")
        return

    print("\n===== CAMPAIGNS =====")
    headers = campaigns[0].keys()
    rows = [list(campaign.values()) for campaign in campaigns]
    print(tabulate(rows[:10], headers=headers, tablefmt="grid"))


def display_impressions(impressions):
    """Displays impression data in table format."""
    if not impressions:
        print("No impression data available.")
        return

    print("\n===== IMPRESSIONS =====")
    headers = impressions[0].keys()
    rows = [list(impression.values()) for impression in impressions]
    print(tabulate(rows[:10], headers=headers, tablefmt="grid"))


def display_products(products):
    """Displays product data in table format."""
    if not products:
        print("No product data available.")
        return

    print("\n===== PRODUCTS =====")
    headers = products[0].keys()
    rows = [list(product.values()) for product in products]
    print(tabulate(rows[:10], headers=headers, tablefmt="grid"))


def display_orders(orders):
    """Displays order data in table format."""
    if not orders:
        print("No order data available.")
        return

    print("\n===== ORDERS (ClickHouse) =====")
    headers = orders[0].keys()
    rows = [list(order.values()) for order in orders]
    print(tabulate(rows[:10], headers=headers, tablefmt="grid"))


def display_combined_data(campaigns, impressions, products=None, orders=None):
    """Combines and displays data from all servers."""
    if not campaigns or not impressions:
        print("Insufficient data for combining.")
        return

    # Group impressions by campaign ID
    impressions_by_campaign = {}
    for impression in impressions:
        campaign_id = impression['campaign_id']
        if campaign_id not in impressions_by_campaign:
            impressions_by_campaign[campaign_id] = []
        impressions_by_campaign[campaign_id].append(impression)

    # Group products by campaign ID (if available)
    products_by_campaign = {}
    if products:
        for product in products:
            campaign_id = product['campaign_id']
            if campaign_id not in products_by_campaign:
                products_by_campaign[campaign_id] = []
            products_by_campaign[campaign_id].append(product)

    # Group orders by campaign ID (if available)
    orders_by_campaign = {}
    if orders:
        for order in orders:
            campaign_id = order['campaign_id']
            if campaign_id not in orders_by_campaign:
                orders_by_campaign[campaign_id] = []
            orders_by_campaign[campaign_id].append(order)

    print("\n===== COMBINED DATA =====")
    for campaign in campaigns[:10]:
        campaign_id = campaign['campaign_id']
        campaign_impressions = impressions_by_campaign.get(campaign_id, [])
        campaign_products = products_by_campaign.get(campaign_id, []) if products else []
        campaign_orders = orders_by_campaign.get(campaign_id, []) if orders else []

        print(f"\nCampaign: {campaign['campaign_name']}")
        print(f"Budget: ${campaign['budget']}")
        print(f"Number of impressions: {len(campaign_impressions)}")

        if campaign_impressions:
            total_clicks = sum(imp['clicks'] for imp in campaign_impressions)
            total_conversions = sum(imp['conversions'] for imp in campaign_impressions)
            total_revenue = sum(imp['revenue'] for imp in campaign_impressions)

            print(f"Total clicks: {total_clicks}")
            print(f"Total conversions: {total_conversions}")
            print(f"Total revenue: ${total_revenue:.2f}")
            print(f"ROI: {(total_revenue / campaign['budget'] * 100):.2f}%")

        if campaign_products:
            print(f"Number of products: {len(campaign_products)}")
            total_price = sum(p['price'] for p in campaign_products)
            categories = set(p['category'] for p in campaign_products)
            print(f"Average product price: ${total_price / len(campaign_products):.2f}")
            print(f"Categories: {', '.join(categories)}")

        if campaign_orders:
            print(f"Number of orders: {len(campaign_orders)}")
            total_quantity = sum(o['quantity'] for o in campaign_orders)
            total_amount = sum(o['total_amount'] for o in campaign_orders)
            print(f"Total quantity sold: {total_quantity}")
            print(f"Total order amount: ${total_amount:.2f}")

        print("-" * 50)


def main():
    try:
        print("Fetching data from database servers...")
        campaigns = get_campaigns()
        impressions = get_impressions()

        # PostgreSQL is optional - handle connection errors gracefully
        try:
            products = get_products()
        except Exception as e:
            print(f"⚠️  Could not connect to PostgreSQL: {e}")
            products = None

        # ClickHouse is optional - handle connection errors gracefully
        try:
            orders = get_orders()
        except Exception as e:
            print(f"⚠️  Could not connect to ClickHouse: {e}")
            orders = None

        # Display data
        display_campaigns(campaigns)
        display_impressions(impressions)
        if products:
            display_products(products)
        if orders:
            display_orders(orders)
        display_combined_data(campaigns, impressions, products, orders)
        
        # Verify data correlation across databases
        verify_data_correlation()

    except Exception as e:
        print(f"Error fetching data: {e}")


if __name__ == "__main__":
    main()