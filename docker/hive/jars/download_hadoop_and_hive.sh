#!/bin/bash
HADOOP_VERSION=3.1.2
HIVE_VERSION=3.1.2

# Download Hadoop
if [ -f hadoop-$HADOOP_VERSION.tar.gz ]; then
    echo "Hadoop already exists"
else
    echo "Downloading Hadoop $HADOOP_VERSION..."
    curl -LO https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz
fi

# Download Hive
if [ -f apache-hive-$HIVE_VERSION-bin.tar.gz ]; then
    echo "Hive already exists"
else
    echo "Downloading Hive $HIVE_VERSION..."
    curl -LO https://archive.apache.org/dist/hive/hive-$HIVE_VERSION/apache-hive-$HIVE_VERSION-bin.tar.gz
fi

echo "Current files:"
ls -l hadoop-$HADOOP_VERSION.tar.gz apache-hive-$HIVE_VERSION-bin.tar.gz
