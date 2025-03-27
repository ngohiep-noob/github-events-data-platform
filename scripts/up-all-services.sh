#!/bin/bash

if ! docker network inspect gharchive_network >/dev/null 2>&1; then
    echo "Creating network gharchive_network..."
    docker network create --driver bridge gharchive_network
fi

echo "Starting Webserver..."
docker compose -f ./api/docker-compose.yml up -d

echo "Starting Minio..."
docker compose -f ./minio/docker-compose.yml up -d

echo "Starting Clickhouse..."
docker compose -f ./clickhouse/docker-compose.yml up -d

echo "Starting Airflow..."
docker compose -f ./airflow/docker-compose.yml up -d

echo "Starting Metabase..."
docker compose -f ./metabase/docker-compose.yml up -d

echo "Starting Spark + Notebook..."
docker compose -f ./spark/docker-compose.yml up -d