#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""
Script for generating SQL files with data for database initialization.
Now creates files directly in mounted volumes.
"""
import os
import random
from datetime import date, timedelta
from faker import Faker

# Initialize Faker with English locale
fake = Faker('en_US')

# Generation parameters
NUM_CAMPAIGNS = 50
IMPRESSIONS_PER_CAMPAIGN = 100

def create_test_data():
    print("🔧 Starting test data creation...")

    # Check available directories
    print("📁 Checking available directories:")
    print(f"  Current working directory: {os.getcwd()}")
    print(f"  Contents of /app: {os.listdir('/app')}")

    # Check mount points
    mariadb1_init_path = '/app/mariadb1_init'
    mariadb2_init_path = '/app/mariadb2_init'
    
    print(f"  Checking {mariadb1_init_path}: exists={os.path.exists(mariadb1_init_path)}, is_dir={os.path.isdir(mariadb1_init_path)}")
    print(f"  Checking {mariadb2_init_path}: exists={os.path.exists(mariadb2_init_path)}, is_dir={os.path.isdir(mariadb2_init_path)}")

    if os.path.exists(mariadb1_init_path):
        print(f"  Contents of {mariadb1_init_path}: {os.listdir(mariadb1_init_path)}")
    if os.path.exists(mariadb2_init_path):
        print(f"  Contents of {mariadb2_init_path}: {os.listdir(mariadb2_init_path)}")

    # SQL for creating campaigns table
    campaigns_sql = """-- Creating campaigns table
CREATE TABLE IF NOT EXISTS campaigns (
    campaign_id INT PRIMARY KEY,
    campaign_name VARCHAR(255) NOT NULL,
    campaign_length INT NOT NULL,
    budget FLOAT NOT NULL
);

-- Inserting campaign data
"""

    # SQL for creating impressions table
    impressions_sql = """-- Creating impressions table
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

    print(f"🔧 Generating {NUM_CAMPAIGNS} campaigns...")
    # Generate campaign data
    for i in range(1, NUM_CAMPAIGNS + 1):
        campaign_length = random.randint(30, 90)
        budget = round(random.uniform(5000, 100000), 2)
        campaign_name = f"Campaign {fake.word().capitalize()} {fake.word().capitalize()}"

        campaigns_data.append({
            'id': i,
            'name': campaign_name,
            'campaign_length': campaign_length
        })

        campaigns_sql += f"""INSERT INTO campaigns (campaign_id, campaign_name, campaign_length, budget)
VALUES ({i}, '{campaign_name}', '{campaign_length}', {budget});\n"""

    print(f"🔧 Generating impressions for campaigns...")
    # Generate impression data
    impression_id = 1
    for campaign in campaigns_data:
        current_length = 0

        for day in range(IMPRESSIONS_PER_CAMPAIGN):
            if current_length > campaign['campaign_length']:
                break

            clicks = random.randint(50, 500)
            conversion_rate = random.uniform(0.01, 0.1)
            conversions = int(clicks * conversion_rate)
            revenue_per_conversion = random.uniform(1, 5)
            revenue = round(conversions * revenue_per_conversion, 2)

            impressions_sql += f"""INSERT INTO impressions (impression_id, campaign_id, days_since_start, clicks, conversions, revenue)
VALUES ({impression_id}, {campaign['id']}, '{current_length}', {clicks}, {conversions}, {revenue});\n"""

            impression_id += 1
            current_length += 1

    print("🔧 Writing SQL files to volumes...")
    # Write SQL to volumes (not local filesystem)
    try:
        with open('/app/mariadb1_init/init.sql', 'w') as f:
            f.write(campaigns_sql)
        print(f"✅ File /app/mariadb1_init/init.sql written successfully")
    except Exception as e:
        print(f"❌ Error writing /app/mariadb1_init/init.sql: {e}")

    try:
        with open('/app/mariadb2_init/init.sql', 'w') as f:
            f.write(impressions_sql)
        print(f"✅ File /app/mariadb2_init/init.sql written successfully")
    except Exception as e:
        print(f"❌ Error writing /app/mariadb2_init/init.sql: {e}")

    # Check what was written
    print("🔍 Checking written files:")
    try:
        mariadb1_size = os.path.getsize('/app/mariadb1_init/init.sql')
        print(f"  /app/mariadb1_init/init.sql: {mariadb1_size} bytes")
    except Exception as e:
        print(f"  ❌ Failed to check /app/mariadb1_init/init.sql: {e}")

    try:
        mariadb2_size = os.path.getsize('/app/mariadb2_init/init.sql')
        print(f"  /app/mariadb2_init/init.sql: {mariadb2_size} bytes")
    except Exception as e:
        print(f"  ❌ Failed to check /app/mariadb2_init/init.sql: {e}")

    print(f"✅ SQL files successfully generated in Docker volumes:")
    print(f"  - /app/mariadb1_init/init.sql (campaigns table with {NUM_CAMPAIGNS} campaigns)")
    print(f"  - /app/mariadb2_init/init.sql (impressions table with {impression_id - 1} records)")

if __name__ == "__main__":
    create_test_data()