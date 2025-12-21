FROM python:3.10

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    sudo \
    openssh-server \
    git \
    postgresql-client \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /var/run/sshd && \
    useradd -m -s /bin/bash manh && \
    echo manh:manhtdxxx | chpasswd && \
    usermod -aG sudo manh && \
    mkdir -p /home/manh/.ssh && \
    chown -R manh:manh /home/manh /home/manh/.ssh && \
    chown -R manh:manh /app

COPY entrypoint.sh /entrypoint.sh
RUN sed -i 's/\r$//' /entrypoint.sh && chmod +x /entrypoint.sh

USER manh