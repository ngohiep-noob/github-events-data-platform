ARG AIRFLOW_IMAGE_NAME=apache/2.10.5-python3.10
FROM ${AIRFLOW_IMAGE_NAME}

ENV AIRFLOW_HOME=/opt/airflow

WORKDIR $AIRFLOW_HOME

USER root
RUN apt-get update -qq && apt-get install vim -qqq && apt-get install -y python3-pip

# Install system dependencies required for xmlsec
RUN apt-get update && apt-get install -y \
    libxml2-dev \
    libxmlsec1-dev \
    pkg-config \
    gcc \
    python3-dev \
    && apt-get clean

ENV JAVA_HOME=/home/jdk-11.0.2

ENV PATH="${JAVA_HOME}/bin/:${PATH}"

RUN DOWNLOAD_URL="https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/openjdk-11.0.2_linux-x64_bin.tar.gz" \
    && mkdir -p "${JAVA_HOME}" \
    && tar xzf "${TMP_DIR}/openjdk-11.0.2_linux-x64_bin.tar.gz" -C "${JAVA_HOME}" --strip-components=1 \
    && rm -rf "${TMP_DIR}" \
    && java --version


COPY requirements.txt .

RUN python3 -m pip install --upgrade pip
RUN python3 -m pip install --no-cache-dir -r requirements.txt

USER $AIRFLOW_UID
