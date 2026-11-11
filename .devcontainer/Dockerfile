FROM python:3.11-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    JAVA_HOME=/opt/java \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update && apt-get install -y --no-install-recommends \
      openjdk-17-jdk-headless git curl ca-certificates build-essential procps bash \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    && python -m spacy download en_core_web_sm

RUN useradd -m -s /bin/bash vscode \
    && mkdir -p /workspace /home/vscode/.ivy2 /home/vscode/.cache/pip \
    && chown -R vscode:vscode /workspace /home/vscode

USER vscode
WORKDIR /workspace
