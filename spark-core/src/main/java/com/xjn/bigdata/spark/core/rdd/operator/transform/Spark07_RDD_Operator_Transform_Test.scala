package com.xjn.bigdata.spark.core.rdd.operator.transform

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * @author shkstart
 * @create 2021-06-30 21:58
 * Test:从服务器日志数据 apache.log 中获取 2015 年 5 月 17 日的请求路径
 */
object Spark07_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[String] = sc.textFile("datas/apache.log")
    rdd.filter(
      log => {
        val split = log.split(" ")
        val time = split(3)
        time.substring(0,5).equals("17/05")
      }
    ).collect().foreach(println)
    sc.stop()
  }
}
