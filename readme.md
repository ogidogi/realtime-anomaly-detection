Hierarchical Temporal Memory for Real-time Anomaly Detection on Spark
==========================
Much of the worlds data is streaming, time-series data, where anomalies give significant information in critical situations.  Detecting anomalies in streaming data is a difficult task, requiring detectors to process data in real-time, and learn while simultaneously making predictions. 
In this presentation we will look at Numenta's novel anomaly detection technique based on an on-line sequence memory algorithm called Hierarchical Temporal Memory (HTM). We will show how this technology can be combined with Big Data stack.
Will demonstrate a small prototype of a real-time anomaly detector based on Spark steaming application which takes the input stream from Kafka, detects anomalies using online learning with HTM and outputs enriched records back into Kafka and visualizes them with a Zeppelin notebook.
# What is done
1)	Everything concerning the intellectual part - the HTM network.

2)	Spark steaming application which takes the input stream from Kafka,
detects anomalies using online learning and outputs enriched records back into Kafka.

3) Prototype of visualization based on zeppelin notebook.

# What is not done yet
1) Optimise parameters and select best properties for real life run

2) Fix maven to not include the properties into the final jar. Add exclude spark libraries from uber jar.

# Run sequence

## One time actions
Start Zookeeper, Kafka and Zeppelin.

If you have Windows, [here](https://dzone.com/articles/running-apache-kafka-on-windows-os) 
is a nice blog about how to run Kafka on Windows.

### Zookeeper
Run Zookeeper:
```
bin\zkServer.cmd
```

### Kafka
Run Kafka:
```
bin\windows\kafka-server-start.bat .\config\server.properties
```

Create kafka topics called "monitoring20" and "monitoringEnriched2", e.g.:
```
bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic monitoring20
bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic monitoringEnriched2
```

## Zeppelin
Run :
```
bin\zeppelin.cmd
```

Create spark2 interpreter from simple spark interpreter, by setting parameters like:
```
PYTHONPATH=%SPARK2_HOME%\python;%SPARK2_HOME%\python\lib\py4j-*-src.zip 
SPARK_HOME=%SPARK2_HOME%
SPARK_MAJOR_VERSION=2
spark.app.name=Zeppelin_Spark2
zeppelin.spark.enableSupportedVersionCheck=false 
```
To descrease amount of records received for zeppelin plots add parameter into spark interpreter
```
spark.streaming.kafka.maxRatePerPartition=10
```

Add following dependencies into spark2 interpreter:
```
org.apache.spark:spark-streaming-kafka-0-10_2.11:jar:2.4.7
org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7
org.apache.kafka:kafka_2.11:2.0.0
org.apache.kafka:kafka-clients:2.0.0
```

Import anomaly-detector-kafka.json notebook into Zeppelin.

In case of problems with Windows path - replace spark-submit on spark-submit2
in bin\interpreter.cmd for spark.

## Running the application during development cycle

1) Start streaming application (com.nm.htm.spark.AnomalyDetector), to wait for incoming data.

2) Start visualizer prototype in zeppelin:
    - Start "Visualizer Section".
    - Start "Streaming Section".
    - Use "Stop Streaming Application" to stop streaming section.

3) Run the com.nm.htm.kafka.TopicGenerator, to fill in raw Kafka topic.
It takes records from csv (data\one_device_2015-2017.csv) and puts them into raw Kafka topic. 
See records schema in com.nm.htm.htm.MonitoringRecord

![Graphs example](/notebook/img/graphs.png?raw=true "Optional Title")


## running anomaly detection on hortonworks
1) set spark version and hdp version
- export SPARK_MAJOR_VERSION=2
- export HDP_VERSION=2.6.1.0-129

for running anomaly-detector(example):

spark-submit  --class com.nm.htm.spark.AnomalyDetector --master local --deploy-mode client --executor-memory 1g  realtime-anomaly-detection-1.0-SNAPSHOT.jar

for running topic-generator(example):

spark-submit  --class com.nm.htm.kafka.TopicGenerator  --master local --deploy-mode client --executor-memory 1g  realtime-anomaly-detection-1.0-SNAPSHOT.jar

In case standalone spark you should add kafka jars to classpath by using --jars option