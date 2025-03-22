#!/bin/bash
echo "Starting Webserver"
docker compose -f ./api/docker-compose.yml up -d

echo "Starting Minio"
docker compose -f ./minio/docker-compose.yml up -d

echo "Starting Clickhouse"
docker compose -f ./clickhouse/docker-compose.yml up -d

echo "Starting Airflow"
docker compose -f ./airflow/docker-compose.yml up -d

