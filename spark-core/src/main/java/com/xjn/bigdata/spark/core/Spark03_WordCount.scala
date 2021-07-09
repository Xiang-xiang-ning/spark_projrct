package com.xjn.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author shkstart
 * @create 2021-06-24 20:54
 */
object Spark03_WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("wordcount")
    val sc = new SparkContext(sparkConf)
    //从文件中读取一行一行数据
    val lines = sc.textFile("datas")
    //扁平化分割
    val words = lines.flatMap(_.split(" "))
    //将一个分割后的元素映射成(元素，1)的map形式
    val wordToOne = words.map(
      word => (word,1)
    )
    //spark提供的方法reduceByKey包含了map和reduce阶段，相同的key的数据，可以对value进行reduce聚合
    val wordToCount: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
    val array:Array[(String,Int)] = wordToCount.collect()
    array.foreach(println)
    sc.stop()
  }
}
