# Spark - poc (New)

## Run

```bash
# check with spark dir
# sc.getConf.get("spark.home")

# run example py job
spark-submit /usr/lib/spark/examples/src/main/python/pi.py

# check spark job log
hdfs dfs -cat /var/log/spark/apps

# build
sbt compile
sbt assembly

# run
spark-submit \
 --class dev.SparkApp1 \
 --deploy-mode cluster \
s3://xxx/sparkJob-assembly-0.1.0-SNAPSHOT.jar

```

## Ref
- Init scala spark project with IntelliJ
	- https://sparkbyexamples.com/spark/spark-setup-run-with-scala-intellij/
	- https://medium.com/@Sushil_Kumar/setting-up-spark-with-scala-development-environment-using-intellij-idea-b22644f73ef1
