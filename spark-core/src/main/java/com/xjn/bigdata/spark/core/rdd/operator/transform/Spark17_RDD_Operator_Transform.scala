package com.xjn.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2021-06-30 21:58
 * foldBykey算子
 *
 */
object Spark17_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[(Char, Int)] = sc.makeRDD(List(('a', 1), ('a', 2), ('b', 3),
      ('b', 4),('b', 5), ('a', 6)),2)
    val combineRDD: RDD[(Char, (Int, Int))] = rdd.combineByKey(
      v => {
        (v, 1)
      },
      (t: (Int, Int), v) => {
        (t._1 + v, t._2 + 1)
      },
      (t1: (Int, Int), t2: (Int, Int)) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )
    combineRDD.map(
      touple => { (touple._1,touple._2._1/touple._2._2) }
    ).collect().foreach(println)
    sc.stop()
  }
}
