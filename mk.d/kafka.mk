run_kz:
	brew services start zookeeper
	brew services start kafka

stop_kz:
	brew services stop zookeeper
	brew services stop kafka

restart_kz:
	brew services restart zookeeper
	brew services restart kafka

make_topic:
	kafka-topics --create -zookeeper localhost:2181 --replication-factor 1  --partitions 1 --topic text_lines
	kafka-topics --create -zookeeper localhost:2181 --replication-factor 1  --partitions 1 --topic topic_ProducerConsumerPartitioner
	kafka-topics --create -zookeeper localhost:2181 --replication-factor 1  --partitions 1 --topic topic_AsyncProducerConsumer		

delete_topics:
	bash script/delete_all_kafka_topics.sh

k_status:
	bash script/kafka_status.sh