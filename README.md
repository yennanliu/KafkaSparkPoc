# KafkaSparkPoc
- Build some stream POC processings via Kafka & Spark
- [Spark](./spark) : main Spark code
- [Kafka](./kafka) : main Kafka code
- Tech stack
	- Scala
	- SBT
	- Spark
	- Kafka
	- IntellJ
	- makefile

## Quick start
- IntellJ
	- dev

- Command line
	- dev

## Run examples

<details>
<summary>Run examples</summary>

```bash
# build 
sbt clean compile
sbt clean assembly
```

- [StreamFromKafkaWithSchema](./spark/src/main/scala/com/yen/dev/StreamFromKafkaWithSchema1.scala)
- Spark stream from  Kafka with Schema and write back to Kafka
```bash
# start zookeeper, kafka
make run_kz
# create kafka topic
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic invoices4
# start producer  
kafka-console-producer --broker-list localhost:9092 --topic invoices4
# and paste some sample data below (sample.json) in the producer console, check the spark-streaming result at /output

# and run the spark-submit script
spark-submit \
 --class com.yen.dev.StreamFromKafkaWithSchema1 \
 target/scala-2.11/spark-app-assembly-1.0.jar
```
- [example.json](./data/SampleData02/samples.json)


- [KafkaSinkDemo1](./spark/src/main/scala/com/yen/dev/KafkaSinkDemo1.scala)
- Spark stream from  Kafka with Schema and write back to Kafka
```bash
# start zookeeper, kafka
make run_kz
# create kafka topic
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic invoices5
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic notifications
# start producer  
kafka-console-producer --broker-list localhost:9092 --topic invoices5
# and run the spark-submit script
spark-submit \
 --class com.yen.dev.KafkaSinkDemo1 \
 target/scala-2.11/spark-app-assembly-1.0.jar
```
</details>

## Ref
- Tutorial & example code
	- https://github.com/LearningJournal/Spark-Streaming-In-Scala
	- https://www.udemy.com/course/apache-spark-streaming-in-scala/
- Other code ref
	- https://github.com/spirom/spark-streaming-with-kafka
	- https://github.com/LearningJournal/Kafka-Streams-Real-time-Stream-Processing
	- https://github.com/confluentinc/kafka-tutorials
	- https://github.com/yennanliu/KafkaHelloWorld