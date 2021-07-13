package com.xjn.bigdata.spark.core.rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2021-06-30 21:58
 */
object Spark02_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1, 2, 3, 4))
    //行动算子只会把RDD转换为其他数据类型，不会转为其他RDD
    //reduce算子:聚集 RDD 中的所有元素，先聚合分区内数据，再聚合分区间数据 1+2+3+4
    val result1: Int = rdd.reduce(_ + _)
    println(result1)
    //collect算子:会把不同分区的数据按照分区顺序采集到Dreiver端内存，形成数组
    val result2: Array[Int] = rdd.collect()
    println(result2.mkString(","))
    //count算子：数据源中的数据个数
    val count: Long = rdd.count()
    println(count)
    //first算子:取数据源中的第一个数据
    val num: Int = rdd.first()
    println(num)
    //take算子:取数据源中的前N个数据
    val array: Array[Int] = rdd.take(3)
    println(array.mkString(","))
    //takeOrdered算子:先排序在按顺序取值
    val rdd1 = sc.makeRDD(List(3, 2, 4, 1))
    val orderedArray = rdd1.takeOrdered(3)(Ordering[Int].reverse)
    println(orderedArray.mkString(","))
    sc.stop()
  }
}
