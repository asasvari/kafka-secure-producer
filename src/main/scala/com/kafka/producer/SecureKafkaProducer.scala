package com.kafka.producer

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.log4j.LogManager


object SecureKafkaProducer {

  @transient lazy val log = LogManager.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      log.error("Usage: SecureKafkaProducer [bootstrap] [topic]")
      log.error("Example: SecureKafkaProducer localhost:9092 topic1")
      System.exit(1)
    }

    val bootstrapServer = args(0)
    val topic = args(1)

    log.info("Creating config properties...")
    val kafkaProperties = new Properties
    kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
    kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer")
    kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    log.info("OK")

    log.info("Creating kafka producer...")
    var i = 0
    val producer = new KafkaProducer[String, String](kafkaProperties)
    while (true) {
      val data = "streamtest-" + i
      val record = new ProducerRecord[String, String](topic, data)
      log.info("Sending record: " + record)
      producer.send(record)
      log.info("OK")
      Thread.sleep(1000)
      i += 1
    }
    log.info("OK")

    log.info("Closing kafka producer...")
    producer.close()
    log.info("OK")
  }
}
