FROM apache/airflow:2.7.3-python3.10

USER root

RUN apt-get update && \
    apt-get install -y openssh-client sshpass && \
    rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Generate SSH keys for airflow user
RUN mkdir -p /home/airflow/.ssh && \
    chmod 700 /home/airflow/.ssh && \
    ssh-keygen -t ecdsa -b 521 -f /home/airflow/.ssh/id_ecdsa -N '' && \
    chmod 600 /home/airflow/.ssh/id_ecdsa