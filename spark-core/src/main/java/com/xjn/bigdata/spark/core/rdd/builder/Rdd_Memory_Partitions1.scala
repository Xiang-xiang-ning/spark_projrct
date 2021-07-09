package com.xjn.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2021-06-29 20:55
 */
object Rdd_Memory_Partitions1 {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(
      List(1, 2, 3, 4, 5),3
    )
    rdd.saveAsTextFile("output/")
    //关闭环境
    sc.stop()
  }
}
