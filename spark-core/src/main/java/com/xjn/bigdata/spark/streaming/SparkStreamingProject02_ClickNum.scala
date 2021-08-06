package com.xjn.bigdata.spark.streaming

import java.text.SimpleDateFormat
import java.util.Date

import com.xjn.bigdata.spark.util.JDBCUtil
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 *
 * @author shkstart
 * @create 2021-08-04 10:26
 */
object SparkStreamingProject02_ClickNum {
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

    val clickdata: DStream[AdClickData] = kafkaDStream.map(
      data => {
        val dataArray: Array[String] = data.value().split(" ")
        AdClickData(dataArray(0), dataArray(1), dataArray(2), dataArray(3), dataArray(4))
      }
    )
    val ds: DStream[((String, String, String,String), Int)] = clickdata.transform(
      rdd => {
        rdd.map(
          click => {
            val sdf = new SimpleDateFormat("yyyy-MM-dd")
            val date = sdf.format(new Date(click.ts.toLong))
            val area = click.area
            val city = click.city
            val adid = click.adid
            ((date, area,city, adid), 1)
          }
        )
      }
    ).reduceByKey(_ + _)

    ds.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          iter => {
            val conn = JDBCUtil.getConnection
            val stat = conn.prepareStatement(
              """
                |insert into area_city_ad_count (dt,area,city,adid,count)
                |values(?,?,?,?,?)
                |ON DUPLICATE KEY
                |UPDATE count = count + ?
                |""".stripMargin)
            iter.foreach{
              case ((date, area,city, adid), cnt) => {
                stat.setString(1,date)
                stat.setString(2,area)
                stat.setString(3,city)
                stat.setString(4,adid)
                stat.setInt(5,cnt)
                stat.setInt(6,cnt)
                stat.executeUpdate()
              }
            }
            stat.close()
            conn.close()
          }
        )
      }
    )


    streamContext.start()
    streamContext.awaitTermination()
  }

  //广告点击数据的样例类
  case class AdClickData(ts:String,area:String,city:String,userid:String,adid:String)
}
