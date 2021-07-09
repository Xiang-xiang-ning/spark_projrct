package com.xjn.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2021-06-30 21:58
 */
object Spark02_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(
      List(1, 2, 3, 4),2
    )
    val mpRDD = rdd.mapPartitions(
      iter => {
        println(">>>>>>>")   //分多少个区就执行多少次
        iter.map(_ * 2)
      }
    )
    mpRDD.collect().foreach(println)
    sc.stop()
  }
}
