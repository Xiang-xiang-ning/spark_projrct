package com.xjn.bigdata.spark.core.rdd.broadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author shkstart
 * @create 2021-07-15 17:08
 */
object Spark01_Broadcast {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("broadcast")
    val sc = new SparkContext(sparkConf)
    val rdd1 = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    val map = mutable.Map[String, Int](("a", 4), ("b", 5), ("c", 6))
    //封装广播变量
    val BroadMap: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)

    rdd1.map {
      touple => {
        //获取广播变量
        val num = BroadMap.value.getOrElse(touple._1, 0)
        (touple._1,(touple._2,num))
      }
    }.collect().foreach(println)

    //这种模式匹配是错误的
//    rdd1 match {
//      case  (word,count): mutable.Map[String,Int] => {
//        val num = map.getOrElse(word, 0)
//        (word,(count,num))
//      }
//    }
  }
}
