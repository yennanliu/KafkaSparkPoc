<h1 align="center">KafkaSparkPoc</h1>
<h4 align="center">Kafka-Spark streaming POC project</h4>

<p align="center">

## Intro
- Projects
	- [Spark](./spark) : Spark application code
	- [Kafka](./kafka) : Kafka application code
- programming language
	- Scala, Java
- Framework
	- Spark, Kafka
- Build tool
	- SBT
- IDE
	- IntellJ

## Scope (Kafka - Spark)
	- Kafka -> Spark
	- Kafka -> Spark -> Kafka
	- Kafka -> Kafka -> Spark
	- Kafka -> Spark -> HDFS
	- Spark -> Kafka

## Scope (Spark)
<details>
<summary>Spark</summary>
	
- Transformation
	- value
		- map : 
			- implement single data point
		- mapPartitions : 
			- implement on data points in the `SAME` partition, may cause OOM
			- good to use when have large memory -> better efficiency
		- mapPartitionsWithIndex
		- flatMap
			- similiar to map, but every input element will be "merged" as an `array`
		- glom
			- make every partition as an array, and form a RDD with type RDD[Array[T]] 
		- groupBy
			- group based on input func, and put values with same key into the same iterator
		- filter
		- sample
		- distinct
		- coalesce
		- repartition
		- sortBy
	- key-value
		- partitionedBy
		- reduceByKey
			- aggregate on key, it has a `pre combine` step before shuffle, return type : RDD[k,v]
			- [reduceByKey1](https://github.com/yennanliu/KafkaSparkPoc/blob/main/spark/src/main/scala/com/yen/sparkBatchBasics/reduceByKey1.scala)
		- groupByKey
			- group by key, and shuffle directly
			- reduceByKey is more preferable than groupByKey in general cases, but still need to consider biz requirements
		- aggregateByKey
			- [aggregateByKey1](https://github.com/yennanliu/KafkaSparkPoc/blob/main/spark/src/main/scala/com/yen/sparkBatchBasics/aggregateByKey1.scala)
		- foldByKey
			-  * General ordering :
				- aggregateByKey -> foldByKey -> reduceByKey
			- [foldByKey1](https://github.com/yennanliu/KafkaSparkPoc/blob/main/spark/src/main/scala/com/yen/sparkBatchBasics/foldByKey1.scala)
		- combineByKey
		- sortedByKey
		- join
		- cogroup
- Action
	- reduce(func)
		- via func aggregate records in same partition, then aggregate records across partitions
	- collect
	- count
	- first
	- take(n)
	- takeOrdered(n)
	- aggregate
	- fold(num)(func)
	- saveAsTextFile
	- saveAsSequenceFile
	- saveAsObjectFile
	- countByKey
	- foreach(func)

</details>

## Structure

```
├── Makefile      - kafka help commands
├── README.md
├── data          - sample data for app demo
├── doc           - collection of project docs
├── exampleCode   - external spark stream example code
├── kafka         - Kafka application source code
├── kafkaJava     - Kafka application source code (Java)
├── mk.d
├── script        - helper scripts
├── spark         - Spark application source code
```

## Build

<details>
<summary>Build</summary>

```bash
# build spark project
cd spark
sbt clean asembly

# build kafka project
cd kafka
sbt clean assembly
```
</details>

## Prerequisites

<details>
<summary>Prerequisites</summary>

- Install
	- Java JDK 1.8
	- Scala
	- Spark 2.X
	- sbt
	- Kafka
	- HDFS (optional)

```bash
# launch kafka
make run_kz

# create kafka topic
kafka-topics --create -zookeeper localhost:2181 --replication-factor 1  --partitions 1 --topic <new_topic>
```

</details>

## Run Basic examples

<details>
<summary>Run Basic examples</summary>

#### 1. [StreamFromKafkaWithSchema](./spark/src/main/scala/com/yen/dev/StreamFromKafkaWithSchema1.scala)
- Spark stream from  Kafka with Schema and write back to Kafka
- [example.json](./data/SampleData02/samples.json)
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
 target/scala-2.11/spark-app-assembly-0.0.1.jar
```

#### 2. [KafkaSinkDemo1](./spark/src/main/scala/com/yen/dev/KafkaSinkDemo1.scala)
- Spark stream from  Kafka with Schema and write back to Kafka
```bash
# start zookeeper, kafka
make run_kz
# create kafka topic
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic invoices6
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic notifications
# start producer  
kafka-console-producer --broker-list localhost:9092 --topic invoices5
# start consumer
kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic notifications 
# and run the spark-submit script
spark-submit \
 --class com.yen.dev.KafkaSinkDemo1 \
 target/scala-2.11/spark-app-assembly-0.0.1.jar
```

#### 3. [KafkaAvroSinkDemo1](./spark/src/main/scala/com/yen/dev/KafkaAvroSinkDemo1.scala)
- Spark stream from Kafka with Schema and write back to Kafka `in avro format`
- [example.json](./data/SampleData02/samples.json)
```bash
# start zookeeper, kafka
make run_kz
# create kafka topic
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic invoices_avro
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic invoice_avro_output
# start producer  
kafka-console-producer --broker-list localhost:9092 --topic invoices_avro
# start consumer
kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic invoice_avro_output 
# and run the spark-submit script
spark-submit \
 --class com.yen.dev.KafkaSinkDemo1 \
 target/scala-2.11/spark-app-assembly-0.0.1.jar
```

#### 4. [TumblingWindowDemo1](./spark/src/main/scala/com/yen/dev/TumblingWindowDemo1.scala)
- Spark stream from Kafka with Schema and process with Tumbling Window for total `buy and sell` values
- [samples.txt](./data/SampleData05/data/samples.txt)
```bash
# start zookeeper, kafka
make run_kz
# create kafka topic
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic trades
# start producer  
kafka-console-producer --broker-list localhost:9092 --topic trades
# and run the spark-submit script
spark-submit \
 --class com.yen.dev.TumblingWindowDemo1 \
 target/scala-2.11/spark-app-assembly-0.0.1.jar
```

#### 4. [streamSocketEventToHDFS](./spark/src/main/scala/com/yen/streamToHDFS/streamSocketEventToHDFS.scala)
- Spark stream event from socket to HDFS file system
```bash
# open a socket at port 9999
nc -lk 9999
# and run the spark-submit script
spark-submit \
 --class com.yen.streamToHDFS.streamSocketEventToHDFS \
 target/scala-2.11/spark-app-assembly-0.0.1.jar

# check the data
hdfs dfs -ls streamSocketEventToHDFS
```

</details>

## Run examples

<details>
<summary>Run examples</summary>

#### 1. Digest Kafka stream and emit to Kafka
```
Event Source -----------> Kafka -----------> Spark Stream  -----------> Kafka 
                                topic = event_raw        topic = event_clean
```
- Kafka : [Producer.scala](./kafka/src/main/scala/com/yen/DigestKafkaEmitKafka/Producer.scala)
```bash
# create topic
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic event_raw
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic event_clean

# start consumer
kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic event_raw

kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic event_clean
```
- Spark : [ProcessAndEmitKafka.scala](./spark/src/main/scala/com/yen/DigestKafkaEmitKafka/ProcessAndEmitKafka.scala)
```bash
spark-submit \
 --class com.yen.DigestKafkaEmitKafka \
 target/scala-2.11/spark-app-assembly-0.0.1.jar
```

</details>

## Ref

<details>
<summary>Ref</summary>
	
- Tutorial & example code
	- https://github.com/LearningJournal/Spark-Streaming-In-Scala
	- https://www.udemy.com/course/apache-spark-streaming-in-scala/

- Other code ref
	- https://github.com/spirom/spark-streaming-with-kafka
	- https://github.com/LearningJournal/Kafka-Streams-Real-time-Stream-Processing
	- https://github.com/confluentinc/kafka-tutorials
	- https://github.com/yennanliu/KafkaHelloWorld

- Online scala code formatter
	- https://scastie.scala-lang.org/

- Kafka
	- [Kafka Consumer multi-threaded instance](https://www.programmersought.com/article/11854192236/)
	- [offsetsForTimes in kafka java api](https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#offsetsForTimes-java.util.Map-)
	- [Java Code Examples for org.apache.kafka.clients.consumer.KafkaConsumer#seek()](https://www.programcreek.com/java-api-examples/?class=org.apache.kafka.clients.consumer.KafkaConsumer&method=seek)

</details>
