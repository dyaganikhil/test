#!/bin/bash

rm -rf /flink-1.14.0/log/*

rm -rf /tmp/flink*

rm -rf /tmp/kafka-logs/

rm -rf /tmp/zookeeper/


nohup /kafka_2.12-3.0.0/bin/zookeeper-server-start.sh /kafka_2.12-3.0.0/config/zookeeper.properties  &
sleep 5
nohup /kafka_2.12-3.0.0/bin/kafka-server-start.sh /kafka_2.12-3.0.0/config/server.properties > kafka_dev.log &
sleep 10
/kafka_2.12-3.0.0/bin/kafka-topics.sh --create --topic CASS_AGGR_EVENT --replication-factor 1 --partitions 5 --bootstrap-server localhost:9092
/kafka_2.12-3.0.0/bin/kafka-topics.sh --create --topic SCRAPER_EVENT --replication-factor 1 --partitions 5 --bootstrap-server localhost:9092
/kafka_2.12-3.0.0/bin/kafka-topics.sh --create --topic FORECAST_DOWNLOAD --replication-factor 1 --partitions 1 --bootstrap-server localhost:9092
sleep 5
/flink-1.14.0/bin/stop-cluster.sh
sleep 5
/flink-1.14.0/bin/start-cluster.sh
sleep 10
nohup /flink-1.14.0/bin/flink run -c com.vassarlabs.application.ApplicationRun /home/backendApp-0.0.1-SNAPSHOT-jar-with-dependencies.jar &
sleep infinity
