# ===== Stage 1: Builder =====
FROM eclipse-temurin:8-jre AS builder

RUN apt-get update && \
    apt-get install -y curl tar && \
    rm -rf /var/lib/apt/lists/*

# Set version
ENV HADOOP_VERSION=3.1.2
ENV HIVE_VERSION=3.1.2
ENV POSTGRES_JDBC_VERSION=42.2.24
ENV AWS_JAVA_SDK_VERSION=1.11.624

# Copy Hadoop & Hive jars
COPY jars/hadoop-${HADOOP_VERSION}.tar.gz /tmp/
COPY jars/apache-hive-${HIVE_VERSION}-bin.tar.gz /tmp/

# Untar Hadoop & Hive
RUN tar --no-same-owner -xvzf /tmp/hadoop-${HADOOP_VERSION}.tar.gz -C /opt/ && \
    tar --no-same-owner -xvzf /tmp/apache-hive-${HIVE_VERSION}-bin.tar.gz -C /opt/ && \
    rm -rf /tmp/*.tar.gz /opt/hadoop-${HADOOP_VERSION}/share/doc

# Download additional required jars
RUN curl -L https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar \
      -o /opt/apache-hive-${HIVE_VERSION}-bin/lib/postgresql-${POSTGRES_JDBC_VERSION}.jar && \
    curl -L "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" \
      -o /opt/hadoop-${HADOOP_VERSION}/share/hadoop/tools/lib/hadoop-aws-${HADOOP_VERSION}.jar && \
    curl -L "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_JAVA_SDK_VERSION}/aws-java-sdk-bundle-${AWS_JAVA_SDK_VERSION}.jar" \
      -o /opt/hadoop-${HADOOP_VERSION}/share/hadoop/tools/lib/aws-java-sdk-bundle-${AWS_JAVA_SDK_VERSION}.jar && \
    cp /opt/hadoop-${HADOOP_VERSION}/share/hadoop/tools/lib/hadoop-aws-${HADOOP_VERSION}.jar \
       /opt/apache-hive-${HIVE_VERSION}-bin/lib/ && \
    cp /opt/hadoop-${HADOOP_VERSION}/share/hadoop/tools/lib/aws-java-sdk-bundle-${AWS_JAVA_SDK_VERSION}.jar \
       /opt/apache-hive-${HIVE_VERSION}-bin/lib/


# ===== Stage 2: Final Image =====
FROM eclipse-temurin:8-jre

RUN apt-get update && \
    apt-get install -y netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*

# Copy Hadoop & Hive from builder
ENV HADOOP_VERSION=3.1.2
ENV HIVE_VERSION=3.1.2
COPY --from=builder /opt/hadoop-${HADOOP_VERSION} /opt/hadoop-${HADOOP_VERSION}
COPY --from=builder /opt/apache-hive-${HIVE_VERSION}-bin /opt/apache-hive-${HIVE_VERSION}-bin

# Set environment variables
ENV HADOOP_HOME=/opt/hadoop-${HADOOP_VERSION}
ENV HIVE_HOME=/opt/apache-hive-${HIVE_VERSION}-bin
ENV PATH=${PATH}:${HADOOP_HOME}/bin:${HIVE_HOME}/bin

# Configuration files
ENV HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
ENV HIVE_CONF_DIR=${HIVE_HOME}/conf
COPY conf/core-site.xml ${HADOOP_CONF_DIR}/core-site.xml
COPY conf/hive-site.xml ${HIVE_CONF_DIR}/hive-site.xml

# Entrypoint
COPY entrypoint.sh /entrypoint.sh
RUN sed -i 's/\r$//' /entrypoint.sh && chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
