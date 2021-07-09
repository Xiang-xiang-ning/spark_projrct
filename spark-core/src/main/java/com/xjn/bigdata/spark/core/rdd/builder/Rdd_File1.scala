package com.xjn.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2021-06-29 20:55
 */
object Rdd_File1 {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)
    //TODO 创建RDD
    val rdd: RDD[(String, String)] = sc.wholeTextFiles("datas/")
    rdd.collect().foreach(println)
    //关闭环境
    sc.stop()
  }
}
