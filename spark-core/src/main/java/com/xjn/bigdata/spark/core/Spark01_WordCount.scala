package com.xjn.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2021-05-24 20:44
 */
object Spark01_WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("wordcount")
    //建立spark连接
    val sc = new SparkContext(sparkConf)

    val lines = sc.textFile("datas")
    val words = lines.flatMap(_.split(" "))
    val wordGroup = words.groupBy(word => word)
    val wordToCount = wordGroup.map {
      case (word,list) => {
        (word,list.size)
      }
    }
    val array:Array[(String,Int)] = wordToCount.collect()
    array.foreach(println)
    sc.stop()
  }

}
