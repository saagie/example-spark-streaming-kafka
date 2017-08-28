package io.saagie.kafka

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scopt.OptionParser

case class Params(hdfsHost: String = "", kafkaBootstrapServer: String = "", kafkaTopics: Seq[String] = Seq())

object Kafka extends App {
  val logger = Logger.getLogger(getClass)

  val parser = new OptionParser[Params]("") {
    head("kafka example", "1.0")

    help("help") text "print this usage text"

    opt[String]("hdfs-host") required() action { (data, conf) =>
      conf.copy(hdfsHost = data)
    } text "URI of the hdfs host. Example: hdfs://IP:8020"

    opt[String]("kafka-bootstrap-server") required() action { (data, conf) =>
      conf.copy(kafkaBootstrapServer = data)
    } text "URI of the kafka server. Example: hdfs://IP:8020"

    opt[Seq[String]]("kafka-topics") valueName "topic1,topic2..." required() action { (data, conf) =>
      conf.copy(kafkaTopics = data)
    } text "Topics to consume"
  }

  parser.parse(args, Params()) match {
    case Some(params) =>
      logger.info(s"Params: $params")
      startStreaming(params)
    case None =>
  }

  def startStreaming(params: Params): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("kafka example")
    val streamingContext = new StreamingContext(sparkConf, Seconds(10))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> params.kafkaBootstrapServer,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "kafka example",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](params.kafkaTopics, kafkaParams)
    )

    val rdds = stream.map(r => (r.key, r.value()))

    rdds.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        rdd.foreach(r => logger.info("key: %s, value %s".format(r._1, r._2)))
      }
    }

    rdds.saveAsTextFiles(params.hdfsHost + "/user/hdfs/kafka")

    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
