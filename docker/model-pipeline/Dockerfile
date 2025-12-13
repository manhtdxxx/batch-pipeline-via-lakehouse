FROM python:3.10-slim


RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        libfreetype6-dev \
        libpng-dev \
        openssh-server \
        sudo \
    && rm -rf /var/lib/apt/lists/*


RUN pip install --upgrade pip setuptools wheel && \
    pip install --no-cache-dir \
    numpy==1.26.0 \
    pandas==2.1.0 \
    matplotlib==3.8.0 \
    seaborn==0.12.2 \
    scikit-learn==1.3.0 \
    tensorflow==2.15.0 \
    trino==0.328.0 \
    mlflow==3.6.0 \
    pyarrow==11.0.0 \
    boto3==1.26.0


RUN useradd -m model_user && \
    echo "model_user:model_pass" | chpasswd && \
    usermod -aG sudo model_user && \
    echo "model_user ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

RUN mkdir -p /var/run/sshd \
    && mkdir -p /home/model_user/.ssh \
    && chown -R model_user:model_user /home/model_user/.ssh

WORKDIR /app
RUN chown -R model_user:model_user /app

USER model_user