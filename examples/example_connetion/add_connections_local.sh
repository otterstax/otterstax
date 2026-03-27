#!/bin/bash

MYSQL_URL="http://0.0.0.0:8085/add_connection"
PG_URL="http://0.0.0.0:8085/add_pg_connection"


for FILE in connection_maria_db1_local.json connection_maria_db2_local.json ; do
    echo "Sending MySQL request with $FILE..."
    curl -X POST "$MYSQL_URL" -H "Content-Type: application/json" -d @"$FILE"
    echo -e "\n"
    sleep 1
done

# PostgreSQL connection
FILE="connection_postgres_local.json"
echo "Sending PostgreSQL request with $FILE..."
curl -X POST "$PG_URL" -H "Content-Type: application/json" -d @"$FILE"
echo -e "\n"