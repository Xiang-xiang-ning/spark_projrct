package com.xjn.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2021-06-24 20:10
 * 持久化checkpoint
 */
object Spark02_RDD_Persist {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("wordcount")
    val sc = new SparkContext(sparkConf)
    sc.setCheckpointDir("cp")
    val rdd: RDD[String] = sc.textFile("datas/2.txt")
    val flatRDD = rdd.flatMap(_.split(" "))
    val mapRDD: RDD[(String, Int)] = flatRDD.map(
      word => {
        println("$$$$$$$$$$$$$$$")
        (word,1)
      }
    )

    mapRDD.checkpoint()
    val reduceRDD = mapRDD.reduceByKey(_ + _)
    reduceRDD.collect().foreach(println)
    println("---------------------")
    val groupRDD = mapRDD.groupByKey()
    groupRDD.collect().foreach(println)
    sc.stop()
  }
}
