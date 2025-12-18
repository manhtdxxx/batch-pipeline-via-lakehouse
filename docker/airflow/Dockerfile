FROM apache/airflow:2.7.3-python3.10

USER root

# Install prerequisites
RUN apt-get update && \
    apt-get install -y openssh-client sshpass && \
    rm -rf /var/lib/apt/lists/*


USER airflow

RUN pip install --no-cache-dir \
    psycopg2-binary==2.9.9 \
    apache-airflow-providers-ssh==3.2.0


# Generate private key & public key from client for SSH connections
RUN ssh-keygen -t ecdsa -b 521 -f /home/airflow/.ssh/id_ecdsa -N ''