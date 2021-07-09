package com.xjn.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2021-06-30 21:58
 */
object Spark04_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[String] = sc.makeRDD(List(
      "hello spark","hello scala"
    ))
    val flatRDD = rdd.flatMap(
      _.split(" ")
    )
    flatRDD.collect().foreach(println)
    sc.stop()
  }
}
