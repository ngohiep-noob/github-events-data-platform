#!/bin/bash
echo "Stopping Webserver..."
docker compose -f ./api/docker-compose.yml down

echo "Stopping Minio..."
docker compose -f ./minio/docker-compose.yml down

echo "Stopping Clickhouse..."
docker compose -f ./clickhouse/docker-compose.yml down

echo "Stopping Airflow..."
docker compose -f ./airflow/docker-compose.yml down

echo "Stopping Metabase..."
docker compose -f ./metabase/docker-compose.yml down
