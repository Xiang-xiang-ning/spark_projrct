package com.xjn.bigdata.spark.core.rdd.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2021-06-30 21:58
 * cogroup算子
 *
 */
object Spark3_RDD_Sql_Operator {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    val rdd1: RDD[(Char, Int)] = sc.makeRDD(List(
      ('a', 1),('a',2),('b', 2), ('c', 3)
    ))
    val rdd2 = sc.makeRDD(List(
      ('b', 6), ('a', 1),('a',9)//,('c', 8)
    ))
    //cogroup = connection + group
    rdd1.cogroup(rdd2).collect().foreach(println)
    sc.stop()
  }
}
