package com.xjn.bigdata.spark.core.rdd.project

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2021-07-16 10:23
 * 需求一：Top10热门品类
 */
object Spark01_Project_HotCategoryTop10 {
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

    //使用leftouterjoin来做，Option类中的元素通过get获取
    val leftjoin1: RDD[(String, (Int, Option[Int]))] = clickRDD.leftOuterJoin(orderRDD)
    val leftjoinRDD: RDD[(String, ((Int, Option[Int]), Option[Int]))] = leftjoin1.leftOuterJoin(payRDD)
    val sortRDD1: RDD[(String, (Int, Int, Int))] = leftjoinRDD.mapValues {
      case ((a, b), c) => {
        (a, b.get, c.get)
      }
    }
    val result1: Array[(String, (Int, Int, Int))] = sortRDD1.sortBy(_._2,false).take(10)
    result1.foreach(println)

    println("******************")

    //使用cogroup来做
    val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = clickRDD.cogroup(orderRDD, payRDD)
    val sortRDD: RDD[(String, (Int, Int, Int))] = cogroupRDD.mapValues {
      case (click, order, pay) => {
        var cli = 0
        if (click.iterator.hasNext) {
          cli = click.iterator.next()
        }
        var ord = 0
        if (order.iterator.hasNext) {
          ord = order.iterator.next()
        }
        var pa = 0
        if (pay.iterator.hasNext) {
          pa = pay.iterator.next()
        }
        (cli, ord, pa)
      }
    }
    val result: Array[(String, (Int, Int, Int))] = sortRDD.sortBy(_._2, false).take(10)
    result.foreach(println)
    sc.stop()
  }
}
