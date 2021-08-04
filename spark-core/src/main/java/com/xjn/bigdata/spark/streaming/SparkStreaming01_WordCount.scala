package com.xjn.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * @author shkstart
 * @create 2021-08-04 10:26
 */
object SparkStreaming01_WordCount {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val streamContext = new StreamingContext(conf, Seconds(3))

    //TODO 逻辑处理
    //监听端口号
    val lines: ReceiverInputDStream[String] = streamContext.socketTextStream("localhost", 9999)
    val flatmap: DStream[String] = lines.flatMap(_.split(" "))
    val result: DStream[(String, Int)] = flatmap.map((_, 1)).reduceByKey(_ + _)
    result.print()

    //TODO 关闭环境
    //由于sparkstreaming采集器是长期执行任务，不能直接关闭
    //如果main方法执行完毕，应用程序也会停止，所以不能让main方法结束
    //streamContext.stop()
    //1.启动采集器
    streamContext.start()
    //2.等待采集器的关闭
    streamContext.awaitTermination()
  }
}
