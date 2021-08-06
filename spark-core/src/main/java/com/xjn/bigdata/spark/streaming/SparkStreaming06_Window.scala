package com.xjn.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * @author shkstart
 * @create 2021-08-04 10:26
 */
object SparkStreaming06_Window {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val streamContext = new StreamingContext(conf, Seconds(3))

    val lines: ReceiverInputDStream[String] = streamContext.socketTextStream("localhost", 9999)
    val mapDS = lines.map((_, 1))
    //第一个参数表示窗口大小，第二个参数表示滑动一次的大小
    //窗口长度一定要是采集周期的整数倍
    val windowDS: DStream[(String,Int)] = mapDS.window(Seconds(6),Seconds(6))
    windowDS.reduceByKey(_+_).print()

    streamContext.start()
    streamContext.awaitTermination()
  }
}
