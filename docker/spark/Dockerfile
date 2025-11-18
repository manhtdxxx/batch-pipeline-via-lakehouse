FROM apache/spark:3.5.0

# Spark Image has their own user 1001, switch to root user
USER root

# Install prerequisites
RUN apt-get update && \
    apt-get install -y curl openssh-server sudo  && \
    rm -rf /var/lib/apt/lists/*

# Jars for Spark to work with Kafka, MinIO, Iceberg
WORKDIR /tmp/jars
RUN curl -L -O https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -L -O https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    curl -L -O https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar && \
    curl -L -O https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    curl -L -O https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/3.5.0/spark-avro_2.12-3.5.0.jar && \
    curl -L -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.426/aws-java-sdk-bundle-1.12.426.jar && \
    curl -L -O https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.0/iceberg-spark-runtime-3.5_2.12-1.5.0.jar && \
    mv *.jar /opt/spark/jars/ && \
    rm -rf /tmp/jars


# Create logs folder and fix permissions
RUN mkdir -p /opt/spark/logs && \
    chown -R 1001:1001 /opt/spark


# Set user for SSH connections
# user 1001 is the default user for spark, but it doesn't have a name
# set password to spark_user
# add spark_user to sudo group (let it execute command as root user)
RUN useradd -u 1001 -m spark_user && \
    echo spark_user:spark_pass | chpasswd && \
    usermod -aG sudo spark_user

    
# Switch back
WORKDIR /opt/spark/app
USER spark_user