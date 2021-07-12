package com.xjn.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2021-06-30 21:58
 * 使用aggregateByKey求相同key的value平均值
 */
object Spark15_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(('a', 1), ('a', 2), ('b', 3),
      ('b', 4),('b', 5), ('a', 6)), 2)
    //aggregateByKey的初始值为一个touple，第一个值为相同key的value总和
    //第二个值为相同key的个数，用第一个值除以第二个值就是平均值
    //aggregateByKey的第二个参数中的第一个为(U, V) => U，V代表K-V键值对的value
    //U为初始值的类型，说明是value和初始值的操作，返回值是初始值的类型
    //第二个参数中的第二个为(U, U) => U，说明最终的返回结果为初始值的结果
    //所以初始值为touple类型返回的也得是touple类型
    val aggregateRDD: RDD[(Char, (Int, Int))] = rdd.aggregateByKey((0, 0))(
      (t, v) => {
        (t._1 + v, t._2 + 1)
      },
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )
    aggregateRDD.map(
      p => { (p._1,p._2._1/p._2._2) }
    ).collect().foreach(println)
    sc.stop()
  }
}
