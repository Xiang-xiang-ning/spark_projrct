package com.xjn.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2021-06-30 21:58
 * aggregateBykey算子
 * aggregateByKey存在函数柯里化，有两个参数列表
 * 第一个参数列表，需要传递一个参数，表示为初始值
 *      主要用于当碰见第一个key的时候，和value进行分区内计算
 * 第二个参数列表需要传递两个参数
 *      第一个参数表示分区内计算规则
 *      第二个参数表示分区间计算规则
 *
 */
object Spark15_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[(Char, Int)] = sc.makeRDD(List(('a', 1), ('a', 2), ('a', 3), ('a', 4),('a', 1), ('a', 2), ('a', 3), ('a', 4)),2)
    //分区内求最大值，分区间求和，第一个分区内最大值为('a',4)，第二个分区也是('a',4)
    //分区间求和：虽然所有的'a'最后都会到一个分区中，但是他们初始的时候不是一个分区，所以是分区间求和就是('a',8)
    rdd.aggregateByKey(0)(
      (x:Int,y:Int) => { math.max(x,y) },
      (x:Int,y:Int) => { x+y }
    ).collect().foreach(println)
    sc.stop()
  }
}
