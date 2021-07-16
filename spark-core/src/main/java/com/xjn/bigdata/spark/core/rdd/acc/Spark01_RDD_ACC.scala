package com.xjn.bigdata.spark.core.rdd.acc

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2021-06-24 20:10
 *         对RDD内的数据求和
 */
object Spark01_RDD_ACC {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("wordcount")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1, 2, 3, 4))
    var sum = 0
    //这种情况是collect将executor端的数据都取回到driver端再循环累加，不是分布式计算，只是在本地计算
    //rdd.collect().foreach(
    //i => sum += i
    //)

    //这种情况sum在executor端累加，但是累加结果并没有被发送到driver端，driver端的sum还是0
    //rdd.foreach(sum+=_)

    println(sum)

    sc.stop()
  }
}
