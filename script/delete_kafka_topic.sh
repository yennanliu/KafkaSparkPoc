#!/bin/sh
# https://github.com/yennanliu/utility_shell/blob/master/kafka/kafka_command.sh

delete_kafka_topic(){
echo "*** delete topic = " $topic
kafka-topics  --zookeeper  127.0.0.1:2181 --topic $topic --delete
}

topic=$1
delete_kafka_topic