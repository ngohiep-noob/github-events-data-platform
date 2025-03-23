# Use the official Metabase image as the base
FROM metabase/metabase:v0.53.x

# Create the plugins directory (if not already present) and download the ClickHouse driver
RUN mkdir -p /plugins && \
    curl -L -o /plugins/clickhouse.jar \
    https://github.com/ClickHouse/metabase-clickhouse-driver/releases/download/1.53.3/clickhouse.metabase-driver.jar

