package com.xjn.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2021-06-30 21:58
 */
object Spark01_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.textFile("datas/apache.log")
    val mapRDD = rdd.map(
      log => {
        val split: Array[String] = log.split(" ")
        split(6)
      }
    )
    mapRDD.collect().foreach(println)
    sc.stop()
  }
}
