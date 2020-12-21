#!/bin/sh

# https://github.com/yennanliu/utility_shell/blob/master/kafka/kafka_command.sh

ZK_IP=127.0.0.1:2181
BOOTSTRAP_SERVER_IP=127.0.0.1:9092

show_topics(){
echo ">>> SHOW ALL KAFKA TOPICS" 
kafka-topics  --zookeeper $ZK_IP --list
}

describe_topics(){
echo ">>> DESCRIBE KAFKA TOPICS"
topics=$(kafka-topics  --zookeeper $ZK_IP --list)
for topic in ${topics[@]} 
do
    echo "topic = " $topic 
    kafka-topics  --zookeeper  $ZK_IP --topic $topic --describe 
done
}

list_groups(){
echo ">>> LIST CONSUMER GROUPS"
kafka-consumer-groups --bootstrap-server  $BOOTSTRAP_SERVER_IP --list
}

describe_consumers(){
echo ">>> DESCRIBE THE CONSUMER"
consumer_groups=$(kafka-consumer-groups  --bootstrap-server  $BOOTSTRAP_SERVER_IP --list)
for consumer_group in ${consumer_groups[@]} 
do
    echo "consumer_group = " $consumer_group 
    kafka-consumer-groups --bootstrap-server $BOOTSTRAP_SERVER_IP --describe --group $consumer_group
done
}

show_topics
#describe_topics
list_groups
#describe_consumers
