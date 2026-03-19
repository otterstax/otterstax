#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax


"""
Script for generating SQL files with data for database initialization.
Creates SQL scripts for MariaDB and PostgreSQL databases.
"""
import os
import random
from pathlib import Path
from faker import Faker

# Initialize Faker with English locale
fake = Faker('en_US')

# Get the directory where this script is located
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent

# Create directories for init scripts (project root - used by docker-compose)
init_dir = PROJECT_ROOT / 'init'
(init_dir / 'mariadb1').mkdir(parents=True, exist_ok=True)
(init_dir / 'mariadb2').mkdir(parents=True, exist_ok=True)
(init_dir / 'postgres').mkdir(parents=True, exist_ok=True)

# Generation parameters
NUM_CAMPAIGNS = 500
IMPRESSIONS_PER_CAMPAIGN = 10000000

# SQL for creating campaigns table
campaigns_sql = """
-- Creating campaigns table
CREATE TABLE IF NOT EXISTS campaigns (
    campaign_id INT PRIMARY KEY,
    campaign_name VARCHAR(255) NOT NULL,
    campaign_length INT NOT NULL,
    budget FLOAT NOT NULL
);

-- Inserting campaign data
"""

# SQL for creating impressions table
impressions_sql = """
-- Creating impressions table
CREATE TABLE IF NOT EXISTS impressions (
    impression_id INT PRIMARY KEY,
    campaign_id INT NOT NULL,
    days_since_start INT NOT NULL,
    clicks INT NOT NULL,
    conversions INT NOT NULL,
    revenue FLOAT NOT NULL
);

-- Inserting impression data
"""

# Generated campaign data for linking
campaigns_data = []

# Generate campaign data
for i in range(1, NUM_CAMPAIGNS + 1):
    # End date from 1 to 3 months after start
    campaign_length = random.randint(30, 90)

    # Budget from 5000 to 100000
    budget = round(random.uniform(5000, 100000), 2)

    # Campaign name
    campaign_name = f"Campaign {fake.word().capitalize()} {fake.word().capitalize()}"

    # Save data for linking with impressions
    campaigns_data.append({
        'id': i,
        'name': campaign_name,
        'campaign_length': campaign_length
    })

    # Add SQL for inserting campaign
    campaigns_sql += f"""INSERT INTO campaigns (campaign_id, campaign_name, campaign_length, budget)
VALUES ({i}, '{campaign_name}', '{campaign_length}', {budget});\n"""

# Generate impression data
impression_id = 1
for campaign in campaigns_data:
    # Create several impression records for each campaign
    current_length = 0

    for day in range(IMPRESSIONS_PER_CAMPAIGN):
        # If we're past the end date, stop
        if current_length > campaign['campaign_length']:
            break

        # Number of clicks from 50 to 500
        clicks = random.randint(50, 500)

        # Conversion rate from 1% to 10%
        conversion_rate = random.uniform(0.01, 0.1)
        conversions = int(clicks * conversion_rate)

        # Revenue from 1 to 5 dollars per conversion
        revenue_per_conversion = random.uniform(1, 5)
        revenue = round(conversions * revenue_per_conversion, 2)

        # Add SQL for inserting impression
        impressions_sql += f"""INSERT INTO impressions (impression_id, campaign_id, days_since_start, clicks, conversions, revenue)
VALUES ({impression_id}, {campaign['id']}, '{current_length}', {clicks}, {conversions}, {revenue});\n"""

        impression_id += 1
        current_length += 1

# Write SQL to files (project root init directory)
with open(init_dir / 'mariadb1' / 'init.sql', 'w') as f:
    f.write(campaigns_sql)

with open(init_dir / 'mariadb2' / 'init.sql', 'w') as f:
    f.write(impressions_sql)

print(f"SQL files successfully generated:")
print(f"- {init_dir}/mariadb1/init.sql (campaigns table with {NUM_CAMPAIGNS} campaigns)")
print(f"- {init_dir}/mariadb2/init.sql (impressions table with {impression_id - 1} records)")

# SQL for creating PostgreSQL products table
postgres_sql = """-- Creating products table for PostgreSQL
CREATE TABLE IF NOT EXISTS products (
    product_id SERIAL PRIMARY KEY,
    campaign_id INT NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    price FLOAT NOT NULL,
    category VARCHAR(100) NOT NULL
);

-- Inserting product data
"""

print(f"Generating products for campaigns...")
product_id = 1
categories = ['Electronics', 'Clothing', 'Food', 'Home', 'Sports', 'Books', 'Toys', 'Beauty']
for campaign in campaigns_data:
    # Each campaign has 2-5 products
    num_products = random.randint(2, 5)
    for _ in range(num_products):
        product_name = f"{fake.word().capitalize()} {fake.word().capitalize()}"
        price = round(random.uniform(9.99, 499.99), 2)
        category = random.choice(categories)

        postgres_sql += f"""INSERT INTO products (campaign_id, product_name, price, category)
VALUES ({campaign['id']}, '{product_name}', {price}, '{category}');\n"""
        product_id += 1

# Write PostgreSQL SQL
with open(init_dir / 'postgres' / 'init.sql', 'w') as f:
    f.write(postgres_sql)

print(f"- {init_dir}/postgres/init.sql (products table with {product_id - 1} records)")
