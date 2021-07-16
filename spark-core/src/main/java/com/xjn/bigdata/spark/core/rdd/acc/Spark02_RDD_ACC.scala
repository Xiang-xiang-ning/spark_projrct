package com.xjn.bigdata.spark.core.rdd.acc

import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2021-06-24 20:10
 *         对RDD内的数据求和
 */
object Spark02_RDD_ACC {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("wordcount")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1, 2, 3, 4))
    var sum = 0
    //spark默认提供简单数据聚合的累加器
    val Accsum: LongAccumulator = sc.longAccumulator("sum")
    rdd.foreach(Accsum.add(_))

    println(Accsum.value)

    sc.stop()
  }
}
