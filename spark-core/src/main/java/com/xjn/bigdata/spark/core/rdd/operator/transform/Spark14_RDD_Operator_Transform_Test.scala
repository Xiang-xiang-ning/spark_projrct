package com.xjn.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2021-06-30 21:58
 * 使用groupByKey+map实现reduceByKey的操作
 */
object Spark14_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(('a', 1), ('a', 1), ('a', 1), ('b', 1), ('b', 1), ('b', 1), ('b', 1), ('a', 1)), 2)
    val groupRDD: RDD[(Char, Iterable[Int])] = rdd.groupByKey(2)
    groupRDD.map(
      touple => { (touple._1,touple._2.sum) }
    ).collect().foreach(println)
    sc.stop()
  }
}
