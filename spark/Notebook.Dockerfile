FROM python:3.10-slim

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

# Install Scala 2.12
ENV SCALA_HOME=/home/scala-2.12.15
ENV PATH="${SCALA_HOME}/bin:${PATH}"
RUN DOWNLOAD_URL="https://downloads.lightbend.com/scala/2.12.15/scala-2.12.15.tgz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/scala-2.12.15.tgz" \
    && mkdir -p "${SCALA_HOME}" \
    && tar xzf "${TMP_DIR}/scala-2.12.15.tgz" -C "${SCALA_HOME}" --strip-components=1 \
    && rm -rf "${TMP_DIR}"

USER 0

# Install the required packages & Jupyter 
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir jupyter 

# Download Clickhouse JDBC driver and save to /app/jars
RUN mkdir -p /app/jars \
    && wget -O /app/jars/clickhouse-jdbc-0.6.3.jar https://github.com/ClickHouse/clickhouse-java/releases/download/v0.6.3/clickhouse-jdbc-0.6.3-all.jar \
    && wget -O /app/jars/clickhouse-spark-runtime-3.5_2.12-0.8.0.jar https://repo1.maven.org/maven2/com/clickhouse/spark/clickhouse-spark-runtime-3.5_2.12/0.8.0/clickhouse-spark-runtime-3.5_2.12-0.8.0.jar

EXPOSE 8888

CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--NotebookApp.token=''"]