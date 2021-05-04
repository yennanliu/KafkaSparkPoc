#!/bin/sh
# https://github.com/yennanliu/utility_shell/blob/master/kafka/kafka_command.sh

delete_all_kafka_topics(){
k_topics=$(kafka-topics  --zookeeper  127.0.0.1:2181 --list)
for topic in ${k_topics}; 
do
    echo "delete topic = " $topic
    kafka-topics  --zookeeper  127.0.0.1:2181 --topic $topic --delete
done
}

delete_all_kafka_topics