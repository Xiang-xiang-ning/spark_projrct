package com.xjn.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2021-06-30 21:58
 * reduceBykey算子
 */
object Spark13_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 1, 1, 4), 2)
    val rdd2: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val toupleRDD = rdd.zip(rdd2)
    toupleRDD.reduceByKey(
      (x:Int,y:Int) => x+y
    ).collect().foreach(println)
    sc.stop()
  }
}
