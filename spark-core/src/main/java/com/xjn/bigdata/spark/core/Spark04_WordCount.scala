package com.xjn.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author shkstart
 * @create 2021-05-24 20:44
 * 11种wordcount方式
 */
object Spark04_WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("wordcount")
    //建立spark连接
    val sc = new SparkContext(sparkConf)

    wordCount9(sc)

    sc.stop()
  }
  //核心groupBy算子
  def wordCount1(sc:SparkContext): Unit ={
    val rdd = sc.makeRDD(List("hello,spark", "hello,scala"))
    val flatRDD: RDD[String] = rdd.flatMap(_.split(","))
    val groupRDD: RDD[(String, Iterable[String])] = flatRDD.groupBy(word => word)
    groupRDD.mapValues(iter => iter.size).collect().foreach(println)
  }
  //核心groupByKey算子
  def wordCount2(sc:SparkContext): Unit ={
    val rdd = sc.makeRDD(List("hello,spark", "hello,scala"))
    val flatRDD: RDD[String] = rdd.flatMap(_.split(","))
    val mapRDD = flatRDD.map((_, 1))
    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()
    groupRDD.mapValues(_.size).collect().foreach(println)
  }
  //核心reduceByKey算子
  def wordCount3(sc:SparkContext): Unit ={
    val rdd = sc.makeRDD(List("hello,spark", "hello,scala"))
    val flatRDD: RDD[String] = rdd.flatMap(_.split(","))
    val mapRDD = flatRDD.map((_, 1))
    mapRDD.reduceByKey(_+_).collect().foreach(println)
  }
  //核心aggregateByKey算子
  def wordCount4(sc:SparkContext): Unit ={
    val rdd = sc.makeRDD(List("hello,spark", "hello,scala"))
    val flatRDD: RDD[String] = rdd.flatMap(_.split(","))
    val mapRDD = flatRDD.map((_, 1))
    mapRDD.aggregateByKey(0)(_+_,_+_).collect().foreach(println)
  }
  //核心foldByKey算子
  def wordCount5(sc:SparkContext): Unit ={
    val rdd = sc.makeRDD(List("hello,spark", "hello,scala"))
    val flatRDD: RDD[String] = rdd.flatMap(_.split(","))
    val mapRDD = flatRDD.map((_, 1))
    mapRDD.foldByKey(0)(_+_).collect().foreach(println)
  }
  //核心combineByKey算子
  def wordCount6(sc:SparkContext): Unit ={
    val rdd = sc.makeRDD(List("hello,spark", "hello,scala"))
    val flatRDD: RDD[String] = rdd.flatMap(_.split(","))
    val mapRDD: RDD[(String, Int)] = flatRDD.map((_, 1))
    mapRDD.combineByKey(
      k => k,
      (k: Int,v: Int ) => { k + v },
      (m: Int,n: Int) => { m + n }
    ).collect().foreach(println)
  }
  //核心countByKey行动算子
  def wordCount7(sc:SparkContext): Unit ={
    val rdd = sc.makeRDD(List("hello,spark", "hello,scala"))
    val flatRDD: RDD[String] = rdd.flatMap(_.split(","))
    val mapRDD: RDD[(String, Int)] = flatRDD.map((_, 1))
    val count: collection.Map[String, Long] = mapRDD.countByKey()
    println(count)
  }
  //核心countByValue行动算子
  def wordCount8(sc:SparkContext): Unit ={
    val rdd = sc.makeRDD(List("hello,spark", "hello,scala"))
    val flatRDD: RDD[String] = rdd.flatMap(_.split(","))
    val count: collection.Map[String, Long] = flatRDD.countByValue()
    println(count)
  }
  //核心reduce行动算子
  def wordCount9(sc:SparkContext): Unit ={
    val rdd = sc.makeRDD(List("hello,spark", "hello,scala"))
    val flatRDD: RDD[String] = rdd.flatMap(_.split(","))
    //def reduce(f: (T, T) => T): T
    //reduce只能聚合一个类型的，因此要转换为map类型
    val mapRDD: RDD[mutable.Map[String, Int]] = flatRDD.map(
      word => {
        mutable.Map[String, Int]((word, 1))
      }
    )
    //reduce聚合是一个一个接着来，每次新来的是map2，来一个map2，就会从map1中查看是否有相同key的
    //如果有map1中的数据的value就会+count更新；没有就把map2的加入到map1中，所以map1中的数据越来越多
    val result: mutable.Map[String, Int] = mapRDD.reduce(
      (map1, map2) => {
        map2.map{
          case (word, count) => {
            val newCount = map1.getOrElse(word, 0) + count //如果map1中有value就是map1的value+1，如果没有就是0+1
            map1.update(word, newCount)
          }
        }
        map1
      }
    )
    println(result)
  }
  //核心aggregate行动算子，原理和reduce类似
  def wordCount10(sc:SparkContext): Unit ={
    val rdd = sc.makeRDD(List("hello,spark", "hello,scala"))
    val flatRDD: RDD[String] = rdd.flatMap(_.split(","))
    val mapRDD: RDD[mutable.Map[String, Int]] = flatRDD.map(
      word => {
        mutable.Map[String, Int]((word, 1))
      }
    )
    val result: mutable.Map[String, Int] = mapRDD.aggregate(mutable.Map(("", 0)))(
      (map1, map2) => {
        map2.map {
          case (word, count) => {
            val newCount = map1.getOrElse(word, 0) + count
            map1.update(word, newCount)
          }
        }
        map1
      },
      (map1, map2) => {
        map2.map {
          case (word, count) => {
            val newCount = map1.getOrElse(word, 0) + count
            map1.update(word, newCount)
          }
        }
        map1
      }
    )
    println(result)
  }
  //核心fold行动算子
  def wordCount11(sc:SparkContext): Unit ={
    val rdd = sc.makeRDD(List("hello,spark", "hello,scala"))
    val flatRDD: RDD[String] = rdd.flatMap(_.split(","))
    val mapRDD: RDD[mutable.Map[String, Int]] = flatRDD.map(
      word => {
        mutable.Map[String, Int]((word, 1))
      }
    )
    val result: mutable.Map[String, Int] = mapRDD.fold(mutable.Map(("", 0)))(
      (map1, map2) => {
        map2.foreach {
          case (word, count) => {
            val newCount = map1.getOrElse(word, 0) + count
            map1.update(word, newCount)
          }
        }
        map1
      }
    )
    println(result)
  }
}
