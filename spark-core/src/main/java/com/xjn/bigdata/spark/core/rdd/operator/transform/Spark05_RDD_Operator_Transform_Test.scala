package com.xjn.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * @author shkstart
 * @create 2021-06-30 21:58
 * Test:计算各区最大值的和
 */
object Spark05_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    val glomRDD = rdd.glom()
    val maxRDD = glomRDD.map(
      list => {
        list.max
      }
    )
    println(maxRDD.collect().sum)
    sc.stop()
  }
}
