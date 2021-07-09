package com.xjn.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2021-06-30 21:58
 */
object Spark01_RDD_Operator_Transform_Par {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1, 2, 3, 4),2)
    val mapRDD = rdd.map(
      num => {
        println(">>>>" + num)
        num
      }
    )
    val mapRDD1 = mapRDD.map(
      num => {
        println("=====" + num)
        num
      }
    )
    mapRDD1.collect()
    sc.stop()
  }
}
