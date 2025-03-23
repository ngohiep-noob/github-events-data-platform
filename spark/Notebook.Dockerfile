FROM python:3.10-slim

ENV CLICKHOUSE_JDBC_VER=0.8.2

WORKDIR /app

USER root

# Install dependencies
RUN apt-get update && apt-get install -y curl wget \
    && apt-get clean

# Install OpenJDK 11
ENV JAVA_HOME=/home/jdk-11.0.2
ENV PATH="${JAVA_HOME}/bin/:${PATH}"
RUN DOWNLOAD_URL="https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/openjdk-11.0.2_linux-x64_bin.tar.gz" \
    && mkdir -p "${JAVA_HOME}" \
    && tar xzf "${TMP_DIR}/openjdk-11.0.2_linux-x64_bin.tar.gz" -C "${JAVA_HOME}" --strip-components=1 \
    && rm -rf "${TMP_DIR}" \
    && java --version

USER 0

# Download Clickhouse JDBC driver and save to /app/jars
RUN mkdir -p /app/jars \
    && wget -O /app/jars/clickhouse-jdbc-${CLICKHOUSE_JDBC_VER}.jar https://github.com/ClickHouse/clickhouse-java/releases/download/v${CLICKHOUSE_JDBC_VER}/clickhouse-jdbc-${CLICKHOUSE_JDBC_VER}.jar

# Install the required packages & Jupyter 
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir jupyter 

EXPOSE 8888

CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--NotebookApp.token=''"]