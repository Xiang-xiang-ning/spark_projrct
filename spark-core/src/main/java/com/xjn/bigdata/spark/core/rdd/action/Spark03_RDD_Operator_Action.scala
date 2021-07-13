package com.xjn.bigdata.spark.core.rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2021-06-30 21:58
 */
object Spark03_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1, 2, 3, 4),2)
    //aggregate算子和转换算子中的aggregateByKey很像
    //区别是：aggregateByKey的初始值只在分区内运算时有效，而aggregate还会在分区间运算有效
    //result = 10+1+2 + 10+3+4 +10 = 40
    val result: Int = rdd.aggregate(10)(_ + _, _ + _)
    println(result)

    val foldresult = rdd.fold(10)(_ + _)
    println(foldresult)

    sc.stop()
  }
}
