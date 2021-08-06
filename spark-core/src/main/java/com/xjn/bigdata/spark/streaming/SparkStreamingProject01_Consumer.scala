package com.xjn.bigdata.spark.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * 消费者测试
 * @author shkstart
 * @create 2021-08-04 10:26
 */
object SparkStreamingProject01_Consumer {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val streamContext = new StreamingContext(conf, Seconds(3))

    //定义 Kafka 参数
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG ->
        "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "atguiguNew",
      "key.deserializer" ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" ->
        "org.apache.kafka.common.serialization.StringDeserializer"
    )
    //读取 Kafka 数据创建 DStream
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream[String, String](streamContext,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Set("atguiguNew"), kafkaPara))

    //取出DStream中的value值
    kafkaDStream.map(_.value()).print()

    streamContext.start()
    streamContext.awaitTermination()
  }
}
