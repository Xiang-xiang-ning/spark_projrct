package com.xjn.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2021-06-30 21:58
 * groupBykey算子
 */
object Spark14_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[(Char, Any)] = sc.makeRDD(List(('a', 'a'), ('a', 2), ('a', 3), ('d', 4)))
    val groupRDD: RDD[(Char, Iterable[Any])] = rdd.groupByKey(2)
    groupRDD.collect().foreach(println)
    sc.stop()
  }
}
