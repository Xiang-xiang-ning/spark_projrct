package com.xjn.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2021-06-30 21:58
 * Test:获取每个分区的最大值
 */
object Spark02_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1, 2, 3, 4),2)
    val mapRDD = rdd.mapPartitions(
      iter => {
        List(iter.max).iterator
      }
    )
    mapRDD.collect().foreach(println)
    sc.stop()
  }
}
