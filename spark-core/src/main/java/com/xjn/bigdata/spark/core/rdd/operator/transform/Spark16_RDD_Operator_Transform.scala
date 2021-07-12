package com.xjn.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2021-06-30 21:58
 * foldBykey算子
 *
 */
object Spark16_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[(Char, Int)] = sc.makeRDD(List(('a', 1), ('a', 2), ('b', 3),
      ('b', 4),('b', 5), ('a', 6)),2)
    //    aggregateByKey也可以设置为分区内和分区间计算规则相同，下例与reduceByKey功能相同
    //    val aggregateRDD: RDD[(Char, Int)] = rdd.aggregateByKey(0)(_ + _, _ + _)
    //    aggregateRDD.collect().foreach(println)
    //    flodByKey默认分区内和分区间计算规则设置相同
    val flodRDD: RDD[(Char, Int)] = rdd.foldByKey(0)(_ + _)
    flodRDD.collect().foreach(println)
    sc.stop()
  }
}
