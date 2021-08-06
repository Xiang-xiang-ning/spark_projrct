package com.xjn.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * @author shkstart
 * @create 2021-08-04 10:26
 */
object SparkStreaming05_Join {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val streamContext = new StreamingContext(conf, Seconds(10))

    val data9999: ReceiverInputDStream[String] = streamContext.socketTextStream("localhost", 9999)
    val data8888: ReceiverInputDStream[String] = streamContext.socketTextStream("localhost", 8888)
    val DStreamJoin: DStream[(String, (Int, Int))] = data9999.map((_, 9)).join(data8888.map((_, 8)))
    DStreamJoin.print()

    streamContext.start()
    streamContext.awaitTermination()
  }
}
