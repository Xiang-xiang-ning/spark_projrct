package com.xjn.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2021-06-30 21:58
 * sortBy算子
 */
object Spark12_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val maprdd: RDD[(Int, Int)] = rdd.map((_, 1))
    //partitionBy是key-value算子，需要touple类型的rdd
    maprdd.partitionBy(new HashPartitioner(2)).saveAsTextFile("output")
    sc.stop()
  }
}
