#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax


# Connection parameters for the first MariaDB server
"""
Script for querying and displaying data from both MariaDB instances and PostgreSQL.
Use --devcon flag to use Docker container connection settings 
(mariadb1:3306, mariadb2:3306, postgres1:5432).
Without the flag, uses localhost connections (0.0.0.0:3101, 0.0.0.0:3102, localhost:5432).
"""
import argparse
import pymysql
import psycopg2
from tabulate import tabulate

# Connection parameters for the first MariaDB server (campaigns)
# Default: localhost connections (for direct access)
CAMPAIGNS_DB_HOST = "0.0.0.0"
CAMPAIGNS_DB_PORT = 3101
CAMPAIGNS_DB_USER = "user1"
CAMPAIGNS_DB_PASSWORD = "password1"
CAMPAIGNS_DB_NAME = "db1"

# Connection parameters for the second MariaDB server (impressions)
# Default: localhost connections (for direct access)
IMPRESSIONS_DB_HOST = "0.0.0.0"
IMPRESSIONS_DB_PORT = 3102
IMPRESSIONS_DB_USER = "user2"
IMPRESSIONS_DB_PASSWORD = "password2"
IMPRESSIONS_DB_NAME = "db2"

# Connection parameters for PostgreSQL server (products)
# Default: localhost connection (for direct access)
POSTGRES_DB_HOST = "0.0.0.0"
POSTGRES_DB_PORT = 3103
POSTGRES_DB_USER = "pguser"
POSTGRES_DB_PASSWORD = "pgpassword"
POSTGRES_DB_NAME = "pgdb"


def setup_connections(use_devcon=False):
    """
    Setup connection parameters based on mode.

    Args:
        use_devcon: If True, use Docker container connection settings
                   (matches examples/example_connetion/connection_maria_db*.json)
                   If False, use localhost connections for direct access
    """
    global CAMPAIGNS_DB_HOST, CAMPAIGNS_DB_PORT
    global IMPRESSIONS_DB_HOST, IMPRESSIONS_DB_PORT
    global POSTGRES_DB_HOST, POSTGRES_DB_PORT

    if use_devcon:
        # Docker container connections (as used in docker-compose)
        # Matches: examples/example_connetion/connection_maria_db1.json
        CAMPAIGNS_DB_HOST = "mariadb1"
        CAMPAIGNS_DB_PORT = 3306

        # Matches: examples/example_connetion/connection_maria_db2.json
        IMPRESSIONS_DB_HOST = "mariadb2"
        IMPRESSIONS_DB_PORT = 3306

        # PostgreSQL Docker container connection
        POSTGRES_DB_HOST = "postgres1"
        POSTGRES_DB_PORT = 5432

        print("Using Docker container connections (--devcon mode)")
        print(f"  Campaigns DB: {CAMPAIGNS_DB_HOST}:{CAMPAIGNS_DB_PORT}")
        print(f"  Impressions DB: {IMPRESSIONS_DB_HOST}:{IMPRESSIONS_DB_PORT}")
        print(f"  PostgreSQL DB: {POSTGRES_DB_HOST}:{POSTGRES_DB_PORT}")
    else:
        # Localhost connections (for direct access outside Docker)
        print("Using localhost connections")
        print(f"  Campaigns DB: {CAMPAIGNS_DB_HOST}:{CAMPAIGNS_DB_PORT}")
        print(f"  Impressions DB: {IMPRESSIONS_DB_HOST}:{IMPRESSIONS_DB_PORT}")
        print(f"  PostgreSQL DB: {POSTGRES_DB_HOST}:{POSTGRES_DB_PORT}")


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


def display_campaigns(campaigns):
    """Displays campaign data in table format (first 5 rows)."""
    if not campaigns:
        print("No campaign data available.")
        return

    print("\n===== CAMPAIGNS (first 5 rows) =====")
    headers = campaigns[0].keys()
    rows = [list(campaign.values()) for campaign in campaigns[:5]]
    print(tabulate(rows, headers=headers, tablefmt="grid"))
    if len(campaigns) > 5:
        print(f"... and {len(campaigns) - 5} more rows")


def display_impressions(impressions):
    """Displays impression data in table format (first 5 rows)."""
    if not impressions:
        print("No impression data available.")
        return

    print("\n===== IMPRESSIONS (first 5 rows) =====")
    headers = impressions[0].keys()
    rows = [list(impression.values()) for impression in impressions[:5]]
    print(tabulate(rows, headers=headers, tablefmt="grid"))
    if len(impressions) > 5:
        print(f"... and {len(impressions) - 5} more rows")


def display_products(products):
    """Displays product data in table format (first 5 rows)."""
    if not products:
        print("No product data available.")
        return

    print("\n===== PRODUCTS (first 5 rows) =====")
    headers = products[0].keys()
    rows = [list(product.values()) for product in products[:5]]
    print(tabulate(rows, headers=headers, tablefmt="grid"))
    if len(products) > 5:
        print(f"... and {len(products) - 5} more rows")


def display_combined_data(campaigns, impressions, products=None):
    """Combines and displays data from both servers (first 5 campaigns)."""
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

    print("\n===== COMBINED DATA (first 5 campaigns) =====")
    for campaign in campaigns[:5]:
        campaign_id = campaign['campaign_id']
        campaign_impressions = impressions_by_campaign.get(campaign_id, [])
        campaign_products = products_by_campaign.get(campaign_id, []) if products else []

        print(f"\nCampaign: {campaign['campaign_name']}")
        print(f"Campaign ID: {campaign_id}")
        print(f"Length: {campaign['campaign_length']} days")
        print(f"Budget: ${campaign['budget']:.2f}")
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

        print("-" * 50)

    if len(campaigns) > 5:
        print(f"... and {len(campaigns) - 5} more campaigns")


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Query and display data from MariaDB instances and PostgreSQL"
    )
    parser.add_argument(
        "--devcon",
        action="store_true",
        help="Use Docker container connection settings (mariadb1:3306, mariadb2:3306, postgres1:5432) "
             "as defined in examples/example_connetion/connection_maria_db*.json"
    )
    args = parser.parse_args()

    # Setup connections based on mode
    setup_connections(use_devcon=args.devcon)

    try:
        print("\nFetching data from database servers...")
        campaigns = get_campaigns()
        impressions = get_impressions()
        
        # PostgreSQL is optional - handle connection errors gracefully
        try:
            products = get_products()
        except Exception as e:
            print(f"⚠️  Could not connect to PostgreSQL: {e}")
            products = None

        # Display data
        display_campaigns(campaigns)
        display_impressions(impressions)
        if products:
            display_products(products)
        display_combined_data(campaigns, impressions, products)

    except Exception as e:
        print(f"Error fetching data: {e}")
        raise


if __name__ == "__main__":
    main()