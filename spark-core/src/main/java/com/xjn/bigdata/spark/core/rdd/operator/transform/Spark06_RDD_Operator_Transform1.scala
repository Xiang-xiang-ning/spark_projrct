package com.xjn.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2021-06-30 21:58
 */
object Spark06_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[String] = sc.makeRDD(List("hello","spark","hello","scala"),2)
    //TODO groupBy算子
    //groupBy会将数据源中的每一个数据进行分组判断，根据返回的分组key进行分组
    //相同key值的数据会放置在一个组中
    val groupRDD = rdd.groupBy(data => {
      data.charAt(0)
    })
    groupRDD.collect().foreach(println)
    sc.stop()
  }
}
