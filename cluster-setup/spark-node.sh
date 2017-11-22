#!/bin/bash

## Configuration
SPARK_VERSION=2.2.0

echo "Installing common software"
apt-get update
apt-get -y install git openjdk-9-jdk-headless unzip vim curl

if [[ ! -d /tmp/spark ]]
then
    echo "Downloading Spark"
    SPARK_URL=http://it.apache.contactlab.it/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop2.7.tgz
    curl -o /tmp/spark.tgz $SPARK_URL

    echo "Unpacking Spark"
    cd /opt
    tar xzvf /tmp/spark.tgz
    mv spark-$SPARK_VERSION-bin-hadoop2.7 spark
fi

