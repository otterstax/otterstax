#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
# Copyright 2025 OtterStax

"""
Скрипт для запроса и отображения данных из обоих экземпляров MariaDB.
"""
import pymysql
from tabulate import tabulate

# Параметры подключения к первому серверу MariaDB
CAMPAIGNS_DB_HOST = "mariadb1"
# CAMPAIGNS_DB_HOST = "0.0.0.0"
# CAMPAIGNS_DB_PORT = 3101
CAMPAIGNS_DB_PORT = 3306
CAMPAIGNS_DB_USER = "user1"
CAMPAIGNS_DB_PASSWORD = "password1"
CAMPAIGNS_DB_NAME = "db1"

# Параметры подключения ко второму серверу MariaDB
IMPRESSIONS_DB_HOST = "mariadb2"
# IMPRESSIONS_DB_HOST = "0.0.0.0"
# IMPRESSIONS_DB_PORT = 3102
IMPRESSIONS_DB_PORT = 3306
IMPRESSIONS_DB_USER = "user2"
IMPRESSIONS_DB_PASSWORD = "password2"
IMPRESSIONS_DB_NAME = "db2"


def get_campaigns():
    """Получает данные о кампаниях из первого сервера MariaDB."""
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
    """Получает данные о показах из второго сервера MariaDB."""
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
    """Отображает данные о кампаниях в табличном виде."""
    if not campaigns:
        print("Нет данных о кампаниях.")
        return

    print("\n===== КАМПАНИИ =====")
    headers = campaigns[0].keys()
    rows = [list(campaign.values()) for campaign in campaigns]
    print(tabulate(rows[:10], headers=headers, tablefmt="grid"))


def display_impressions(impressions):
    """Отображает данные о показах в табличном виде."""
    if not impressions:
        print("Нет данных о показах.")
        return

    print("\n===== ПОКАЗЫ =====")
    headers = impressions[0].keys()
    rows = [list(impression.values()) for impression in impressions]
    print(tabulate(rows[:10], headers=headers, tablefmt="grid"))


def display_combined_data(campaigns, impressions):
    """Объединяет и отображает данные из обоих серверов."""
    if not campaigns or not impressions:
        print("Недостаточно данных для объединения.")
        return

    # Группируем показы по ID кампании
    impressions_by_campaign = {}
    for impression in impressions:
        campaign_id = impression['campaign_id']
        if campaign_id not in impressions_by_campaign:
            impressions_by_campaign[campaign_id] = []
        impressions_by_campaign[campaign_id].append(impression)

    print("\n===== ОБЪЕДИНЕННЫЕ ДАННЫЕ =====")
    for campaign in campaigns[:10]:
        campaign_id = campaign['campaign_id']
        campaign_impressions = impressions_by_campaign.get(campaign_id, [])

        print(f"\nКампания: {campaign['campaign_name']}")
        print(f"Бюджет: ${campaign['budget']}")
        print(f"Количество показов: {len(campaign_impressions)}")

        if campaign_impressions:
            total_clicks = sum(imp['clicks'] for imp in campaign_impressions)
            total_conversions = sum(imp['conversions'] for imp in campaign_impressions)
            total_revenue = sum(imp['revenue'] for imp in campaign_impressions)

            print(f"Всего кликов: {total_clicks}")
            print(f"Всего конверсий: {total_conversions}")
            print(f"Общий доход: ${total_revenue:.2f}")
            print(f"ROI: {(total_revenue / campaign['budget'] * 100):.2f}%")

        print("-" * 50)


def main():
    try:
        print("Получение данных с серверов MariaDB...")
        campaigns = get_campaigns()
        impressions = get_impressions()

        # Отображаем данные
        display_campaigns(campaigns)
        display_impressions(impressions)
        display_combined_data(campaigns, impressions)

    except Exception as e:
        print(f"Ошибка при получении данных: {e}")


if __name__ == "__main__":
    main()