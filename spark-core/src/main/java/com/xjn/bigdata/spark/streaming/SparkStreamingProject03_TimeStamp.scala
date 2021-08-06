package com.xjn.bigdata.spark.streaming

import java.io.{File, FileWriter, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import com.xjn.bigdata.spark.streaming.SparkStreamingProject02_ClickNum.AdClickData
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer


/**统计不同时间段广告点击数并在网页显示
 *
 * @author shkstart
 * @create 2021-08-04 10:26
 */
object SparkStreamingProject03_TimeStamp {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val streamContext = new StreamingContext(conf, Seconds(5))

    streamContext.checkpoint("project03")

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

    val clickData: DStream[AdClickData] = kafkaDStream.map(
      data => {
        val dataArray: Array[String] = data.value().split(" ")
        AdClickData(dataArray(0), dataArray(1), dataArray(2), dataArray(3), dataArray(4))
      }
    )
//    val result = clickData.map(
//      data  => {
//            val time = data.ts.toLong / 10000 * 10000
//            (time, 1)
//          }
//        ).reduceByKeyAndWindow(
//      (x: Int, y: Int) => {
//        x + y
//      },
//      (x: Int, y: Int) => {
//        x - y
//      },
//      Seconds(60),
//      Seconds(10)
//    )

    //直接使用map方法就可以了，sparkstreaming的map方法就是对rdd进行map
    //reduceByKeyAndWindow是sparkstreaming的方法
    val result: DStream[(Long, Int)] = clickData.transform(
      rdd => {
        rdd.map(
          data => {
            val time = data.ts.toLong / 10000 * 10000
            (time, 1)
          }
        )
      }
    ).reduceByKeyAndWindow(
      (x: Int, y: Int) => {
        x + y
      },
      (x: Int, y: Int) => {
        x - y
      },
      Seconds(60),
      Seconds(10)
    )

//    result.print()
    result.foreachRDD(
      rdd => {
        val jsonBuf = new ListBuffer[String]()
        val datas: Array[(Long, Int)] = rdd.sortByKey(true).collect()
        datas.foreach{
          case (time,cnt) => {
            val ts = new SimpleDateFormat("mm:ss").format(new Date(time.toLong))
            jsonBuf.append(s"""{ "xtime":"${ts}", "yval":"${cnt}" }""")
          }
        }
        val out = new PrintWriter(new FileWriter(new File("D:\\IDEAworkspace\\spark_projrct\\datas\\adclick\\adclick.json")))
        out.print("["+jsonBuf.mkString(",")+"]")
        out.flush()
        out.close()
      }
    )


    streamContext.start()
    streamContext.awaitTermination()
  }
  //广告点击数据的样例类
  case class AdClickData(ts:String,area:String,city:String,userid:String,adid:String)
}
