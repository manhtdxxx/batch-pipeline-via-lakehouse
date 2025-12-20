# Stage 1: Build stage
FROM python:3.10-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        libfreetype6-dev \
        libpng-dev \
        libhdf5-dev \
        zlib1g-dev \
        libjpeg-dev \
        libblas-dev \
        liblapack-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python packages
WORKDIR /install
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --prefix=/install --no-cache-dir -r requirements.txt


# Stage 2: Runtime image
FROM python:3.10-slim

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        sudo \
        openssh-server \
        git \
        postgresql-client \
        libgomp1 \
        libstdc++6 \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Copy Python packages from builder
COPY --from=builder /install /usr/local

# Create user for running the model service and SSH
RUN mkdir -p /var/run/sshd && \
    useradd -m -s /bin/bash manh && \
    echo manh:manhtdxxx | chpasswd && \
    usermod -aG sudo manh && \
    mkdir -p /home/manh/.ssh && \
    chown -R manh:manh /home/manh /home/manh/.ssh && \
    mkdir -p /app && \
    chown -R manh:manh /app

WORKDIR /app
USER manh
