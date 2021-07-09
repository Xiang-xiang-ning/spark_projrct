package com.xjn.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2021-06-29 20:55
 */
object Rdd_Memory {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)
    //TODO 创建RDD
    //从内存中创建RDD，将内存中集合的数据作为处理的数据源
    val seq: Seq[Int] = Seq[Int](1, 2, 3, 4)
    sc.makeRDD(seq).collect().foreach(println)
    //关闭环境
    sc.stop()
  }
}
