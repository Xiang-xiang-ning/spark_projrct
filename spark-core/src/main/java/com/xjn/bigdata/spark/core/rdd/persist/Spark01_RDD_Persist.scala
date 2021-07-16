package com.xjn.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2021-06-24 20:10
 * 持久化
 */
object Spark01_RDD_Persist {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("wordcount")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[String] = sc.textFile("datas/2.txt")
    val flatRDD = rdd.flatMap(_.split(" "))
    val mapRDD: RDD[(String, Int)] = flatRDD.map(
      word => {
        println("$$$$$$$$$$$$$$$")
        (word,1)
      }
    )
    //如果不持久化，多次使用RDD也需要重新开始运行，因为RDD不存储数据
    //cache是持久化到内存
    //mapRDD.cache()
    //persist可以选择持久化等级
    mapRDD.persist(StorageLevel.DISK_ONLY)
    val reduceRDD = mapRDD.reduceByKey(_ + _)
    reduceRDD.collect().foreach(println)
    println("---------------------")
    val groupRDD = mapRDD.groupByKey()
    groupRDD.collect().foreach(println)
    sc.stop()
  }
}
