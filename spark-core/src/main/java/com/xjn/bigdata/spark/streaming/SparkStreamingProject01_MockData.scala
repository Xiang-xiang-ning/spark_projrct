package com.xjn.bigdata.spark.streaming

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}


import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
 * @author shkstart
 * @create 2021-08-05 17:21
 */
object SparkStreamingProject01_MockData {
  def main(args: Array[String]): Unit = {
    //生成模拟数据
    //格式：timestamp area city userid adid
    //含义：时间戳     地区 城市  用户   广告

    //流程：Application => Kafka => SparkStreaming => Analysis

    // 创建配置对象
    val prop = new Properties()
    // 添加配置
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092")
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](prop)

    while (true) {
      mockdata().foreach(
        data => {
          //向kafka中生产数据
          val record = new ProducerRecord[String, String]("atguiguNew",data)
          producer.send(record)
          println(data)
        }
      )
      Thread.sleep(2000)
    }
  }

  def mockdata(): ListBuffer[String] = {
    val list = ListBuffer[String]()
    val listArea = ListBuffer[String]("华东","华北","华南")
    val listCity = ListBuffer[String]("北京","上海","深圳","杭州")
    for (i <- 1 to new Random().nextInt(50)) {
      val area = listArea(new Random().nextInt(3))
      val city = listCity(new Random().nextInt(4))
      val userid = new Random().nextInt(6)+1
      val adid = new Random().nextInt(6)+1
      list.append(s"${System.currentTimeMillis()} ${area} ${city} ${userid} ${adid}")
    }
    list
  }
}
