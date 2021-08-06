package com.xjn.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * @author shkstart
 * @create 2021-08-04 10:26
 */
object SparkStreaming07_reduceByKeyAndWindow {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val streamContext = new StreamingContext(conf, Seconds(1))

    streamContext.checkpoint("window_checkpoint")

    val lines: ReceiverInputDStream[String] = streamContext.socketTextStream("localhost", 9999)
    val mapDS = lines.map((_, 1))

    val windowDS: DStream[(String, Int)] = mapDS.reduceByKeyAndWindow(
      (x: Int, y: Int) => {
        x + y
      },
      (x: Int, y: Int) => {
        x - y
      },
      Seconds(3),
      Seconds(2)
    )
    windowDS.print()

    streamContext.start()
    streamContext.awaitTermination()
  }
}
