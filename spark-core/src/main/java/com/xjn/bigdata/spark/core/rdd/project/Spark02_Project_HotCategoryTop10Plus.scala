package com.xjn.bigdata.spark.core.rdd.project

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2021-07-16 10:23
 * 需求一：Top10热门品类
 */
object Spark02_Project_HotCategoryTop10Plus {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("Project1")
    val sc = new SparkContext(sparkConf)

    //1.读取原始日志数据
    val rdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    rdd.cache()

    //2.统计品类的点击数据(品类ID，点击数)
    val clickfilterRDD: RDD[String] = rdd.filter(
      line => {
        val datas = line.split("_")
        datas(6) != "-1"
      }
    )
    val clickRDD: RDD[(String, Int)] = clickfilterRDD.map(
      line => {
        val split: Array[String] = line.split("_")
        (split(6), 1)
      }
    ).reduceByKey(_ + _)
    //3.统计品类的下单数据(品类ID，下单数)
    val orderfilterRDD: RDD[String] = rdd.filter(
      line => {
        val datas = line.split("_")
        datas(8) != "null"
      }
    )
    val splitRDD = orderfilterRDD.map(
      data => {
        val split = data.split("_")
        split(8)
      }
    )
    val orderRDD: RDD[(String, Int)] = splitRDD.flatMap(_.split(",")).map((_, 1)).reduceByKey(_ + _)
    //4.统计品类的支付数据(品类ID，支付数)
    val payfilterRDD: RDD[String] = rdd.filter(
      line => {
        val datas = line.split("_")
        datas(10) != "null"
      }
    )
    val paysplitRDD = payfilterRDD.map(
      data => {
        val split = data.split("_")
        split(10)
      }
    )
    val payRDD: RDD[(String, Int)] = paysplitRDD.flatMap(_.split(",")).map((_, 1)).reduceByKey(_ + _)
    //5.按照品类排序，并且取前10名
    //优先级为点击>下单>支付
    //可以使用元组排序(元组排序先比较第一个然后以此类推)
    //(品类ID,(点击数，下单数，支付数))
    //(游戏品类，点击数) => (游戏品类，(点击数，0，0))
    //(游戏品类，下单数) => (游戏品类，(0，下单数，0))
    //(游戏品类，支付数) => (游戏品类，(0，0，支付数))
    //union一下，然后reduce聚合 => (游戏品类，(点击数，下单数，支付数))

    val clickplus: RDD[(String, (Int, Int, Int))] = clickRDD.map {
      case (word, count) => {
        (word, (count, 0, 0))
      }
    }
    val orderplus: RDD[(String, (Int, Int, Int))] = orderRDD.map {
      case (word, count) => {
        (word, (0, count, 0))
      }
    }
    val payplus: RDD[(String, (Int, Int, Int))] = payRDD.map {
      case (word, count) => {
        (word, (0, 0, count))
      }
    }
    val unionRDD: RDD[(String, (Int, Int, Int))] = clickplus.union(orderplus).union(payplus)
    val reduceRDD: RDD[(String, (Int, Int, Int))] = unionRDD.reduceByKey(
      (a, b) => {
        (a._1 + b._1, a._2 + b._2, a._3 + b._3)
      }
    )
    reduceRDD.sortBy(_._2,false).take(10).foreach(println)


    sc.stop()
  }
}
