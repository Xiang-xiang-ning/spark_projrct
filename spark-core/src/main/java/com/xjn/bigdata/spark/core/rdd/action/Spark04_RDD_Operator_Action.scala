package com.xjn.bigdata.spark.core.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2021-06-30 21:58
 */
object Spark04_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    //计算不同值出现的次数
    //Map(1 -> 2, 2 -> 2, 3 -> 3, 5 -> 1)
    val rdd1 = sc.makeRDD(List(1, 1, 2, 3, 3, 3, 5, 2))
    val count: collection.Map[Int, Long] = rdd1.countByValue()
    println(count)
    val rdd2: RDD[(Int, Char)] = sc.makeRDD(List((1, 'a'), (1, 'b'), (2, 'a'), (2, 'c'), (3, 'c')))
    //countByKey算子只有key出现的个数有关，与value的值无关
    val countByKey: collection.Map[Int, Long] = rdd2.countByKey()
    println(countByKey)
    sc.stop()
  }
}
