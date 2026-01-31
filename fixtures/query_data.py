#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""
Script for querying and displaying data from both MariaDB instances.
"""
import pymysql
from tabulate import tabulate

# Connection parameters for the first MariaDB server
CAMPAIGNS_DB_HOST = "0.0.0.0"
CAMPAIGNS_DB_PORT = 3101
CAMPAIGNS_DB_USER = "user1"
CAMPAIGNS_DB_PASSWORD = "password1"
CAMPAIGNS_DB_NAME = "db1"

# Connection parameters for the second MariaDB server
IMPRESSIONS_DB_HOST = "0.0.0.0"
IMPRESSIONS_DB_PORT = 3102
IMPRESSIONS_DB_USER = "user2"
IMPRESSIONS_DB_PASSWORD = "password2"
IMPRESSIONS_DB_NAME = "db2"


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


def display_campaigns(campaigns):
    """Displays campaign data in table format."""
    if not campaigns:
        print("No campaign data available.")
        return

    print("\n===== CAMPAIGNS =====")
    headers = campaigns[0].keys()
    rows = [list(campaign.values()) for campaign in campaigns]
    print(tabulate(rows, headers=headers, tablefmt="grid"))


def display_impressions(impressions):
    """Displays impression data in table format."""
    if not impressions:
        print("No impression data available.")
        return

    print("\n===== IMPRESSIONS =====")
    headers = impressions[0].keys()
    rows = [list(impression.values()) for impression in impressions]
    print(tabulate(rows, headers=headers, tablefmt="grid"))


def display_combined_data(campaigns, impressions):
    """Combines and displays data from both servers."""
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

    print("\n===== COMBINED DATA =====")
    for campaign in campaigns:
        campaign_id = campaign['campaign_id']
        campaign_impressions = impressions_by_campaign.get(campaign_id, [])

        print(f"\nCampaign: {campaign['campaign_name']}")
        print(f"Dates: {campaign['start_date']} - {campaign['end_date']}")
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

        print("-" * 50)


def main():
    try:
        print("Fetching data from MariaDB servers...")
        campaigns = get_campaigns()
        impressions = get_impressions()

        # Display data
        display_campaigns(campaigns)
        display_impressions(impressions)
        display_combined_data(campaigns, impressions)

    except Exception as e:
        print(f"Error fetching data: {e}")


if __name__ == "__main__":
    main()