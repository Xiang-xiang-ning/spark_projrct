package com.xjn.bigdata.spark.core.rdd.acc

import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2021-06-24 20:10
 * 错误示范wordcount
 */
object Spark03_RDD_ACC_WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("wordcount")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List("hello","spark","scala","hello"))
    //错误示范
    var iHello = 0
    var iScala = 0
    var iSpark = 0
    val iHello1 = sc.longAccumulator("iHello")
    val iScala1 = sc.longAccumulator("iScala")
    val iSpark1 = sc.longAccumulator("iSpark")
    rdd.foreach(
      word => word match {
        case "hello" => iHello1.add(1)
        case "spark" => iSpark1.add(1)
        case "scala" => iScala1.add(1)
      }
    )
    println("hello:"+iHello1.value)
    println("scala:"+iScala1.value)
    println("spark:"+iSpark1.value)
    sc.stop()
  }
}
