package com.xjn.bigdata.spark.core.rdd.acc

import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author shkstart
 * @create 2021-06-24 20:10
 *         对RDD内的数据求和
 */
object Spark04_RDD_ACC_WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("wordcount")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List("hello","spark","hello","scala"))
    val acc = new MyAccumulator
    sc.register(acc,"myacc")
    rdd.foreach(
      word => acc.add(word)
    )
    println(acc.value)
    sc.stop()
  }
}

class MyAccumulator extends AccumulatorV2[String,mutable.Map[String,Int]]{

  private val wordMap = mutable.Map[String,Int]()

  //判断是否为初始状态
  override def isZero: Boolean = {
    wordMap.isEmpty
  }

  //复制一个新的累加器
  override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
    new MyAccumulator
  }

  //重置累加器
  override def reset(): Unit = {
    wordMap.clear()
  }

  //获取累加器需要计算的值
  override def add(v: String): Unit = {
    val newCount: Int = wordMap.getOrElse(v, 0) + 1
    wordMap.update(v,newCount)
  }

  //Driver端合并多个累加器
  override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
    var map1 = wordMap
    var map2 = other.value
    map2.foreach(
      word => {
        val newCount = map1.getOrElse(word._1,0)+word._2
        map1.update(word._1,newCount)
      }
    )
  }

  //累加器结果
  override def value: mutable.Map[String, Int] = {
    wordMap
  }
}