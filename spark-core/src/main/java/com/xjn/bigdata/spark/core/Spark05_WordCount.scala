package com.xjn.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2021-06-24 20:54
 * Test 血缘关系
 */
object Spark05_WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("wordcount")
    val sc = new SparkContext(sparkConf)
    val lines = sc.textFile("datas/2.txt")
    println(lines.toDebugString)
    println("*****************")

    val words = lines.flatMap(_.split(" "))
    println(words.toDebugString)
    println("*****************")

    val wordToOne = words.map(
      word => (word,1)
    )
    println(wordToOne.toDebugString)
    println("*****************")

    val wordToCount: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
    println(wordToCount.toDebugString)
    println("*****************")

    val array:Array[(String,Int)] = wordToCount.collect()
    array.foreach(println)
    sc.stop()
  }
}
