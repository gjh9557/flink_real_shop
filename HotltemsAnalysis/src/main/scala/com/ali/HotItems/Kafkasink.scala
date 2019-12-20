package com.ali.HotItems

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object Kafkasink {
  def main(args: Array[String]): Unit = {
writeToKafka("hotitems")
  }
  def writeToKafka(topic:String): Unit ={
    val props = new Properties()
    props.put("bootstrap.servers", Constant.Constant.bootstarp_servers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    val bufferedSource = io.Source.fromFile("E:\\qqq\\days\\12.20\\real_shop\\HotltemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    for (line <- bufferedSource.getLines) {
      val record = new ProducerRecord[String, String](topic, line)
      producer.send(record)
      println("发送一条")
    }
    producer.close()
  }
}
