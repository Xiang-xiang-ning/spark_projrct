package com.xjn.bigdata.spark.core.rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2021-06-30 21:58
 */
object Spark05_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1, 2, 3, 4),2)
    //这里的foreach是driver端内存集合的循环遍历方法
    rdd.collect().foreach(println)
    println("***********")
    //这里的foreach是Executor端内存数据打印
    rdd.foreach(println)
    sc.stop()
  }
}
