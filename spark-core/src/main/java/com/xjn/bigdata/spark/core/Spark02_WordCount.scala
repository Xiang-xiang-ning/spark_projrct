package com.xjn.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2021-06-24 20:10
 */
object Spark02_WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("wordcount")
    val sc = new SparkContext(sparkConf)

    val lines = sc.textFile("datas")
    val words = lines.flatMap(_.split(" "))

    val wordToOne = words.map(
      word => (word,1)
    )

    val groupRDD: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(
      t => t._1
    )
    val wordToCount = groupRDD.map {
      case (word,list) => {
        list.reduce(
          (t1,t2) => {
            (t2._1,t1._2 + t2._2)
          }
        )
      }
    }
    val array:Array[(String,Int)] = wordToCount.collect()
    array.foreach(println)
    sc.stop()
  }
}
