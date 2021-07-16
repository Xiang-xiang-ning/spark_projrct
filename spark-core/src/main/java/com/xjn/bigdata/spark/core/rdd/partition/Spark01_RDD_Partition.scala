package com.xjn.bigdata.spark.core.rdd.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2021-07-15 11:53
 */
object Spark01_RDD_Partition {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("partition")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[(String, String)] = sc.makeRDD(List(
      ("basketball", "*****"),
      ("football", "*****"),
      ("baseball", "*****"),
      ("basketball", "******"),
      ("football","*******"),
      ("soccer","*******")
    ))
    rdd.partitionBy(new MyPartitioner).saveAsTextFile("output")

    sc.stop()

  }
}

class MyPartitioner extends Partitioner{
  override def numPartitions: Int = 3

  override def getPartition(key: Any): Int = {
    key match {
      case "basketball" => 0
      case "football" => 1
      case _ => 2
    }
  }
}
