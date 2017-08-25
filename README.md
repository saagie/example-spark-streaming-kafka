# Example for consuming from Kafka with Spark Streaming

Package: sbt clean assembly then use the generated jar.

Usage:

```spark-submit example-spark-streaming-kafka.jar --hdfs-host hdfs://hdfshost:8020 --kafka-bootstrap-server kafkaserver:9092 --kafka-topics test```

Notes:

A working Kafka should be available.

