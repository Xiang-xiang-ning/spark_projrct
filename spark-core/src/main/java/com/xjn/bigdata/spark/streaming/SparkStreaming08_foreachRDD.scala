package com.xjn.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author shkstart
 * @create 2021-08-05 17:21
 */
object SparkStreaming08_foreachRDD {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")
    val streamContext = new StreamingContext(conf, Seconds(3))
    val lines = streamContext.socketTextStream("localhost", 8888)
    //foreachRDD是一种OutputDStream，可以拿到一个个底层的RDD并对其操作
    lines.foreachRDD(
      rdd => {
        rdd.map((_,1)).collect().foreach(println)

      }
    )

    streamContext.start()
    streamContext.awaitTermination()

  }

}
