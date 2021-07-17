package com.xjn.bigdata.spark.core.rdd.project

import java.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2021-07-16 10:23
 * 需求一：Top10热门品类
 */
object Spark03_Project_HotCategoryTop10Pro {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("Project1")
    val sc = new SparkContext(sparkConf)

    //1.读取原始日志数据
    val rdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")

    //因为每条数据只会发生点击，下单和支付中的一种，所以可以用if-else
    //此处只能用flatmap不能用map的原因：下单和支付两个else if语句中返回的都是Array数组(下单和支付可以多个商品一起)
    //所以也要将点击包装成list集合，然后一起用flatmap打散
    val mapRDD: RDD[(String, (Int, Int, Int))] = rdd.flatMap(
      word => {
        val split = word.split("_")
        if (split(6) != "-1") {
          List((split(6), (1, 0, 0)))
        } else if (split(8) != "null") {
          val order = split(8).split(",")
          order.map(
            id => (id, (0, 1, 0))
          )
        } else if (split(10) != "null") {
          val pay = split(10).split(",")
          pay.map(
            id => (id, (0, 0, 1))
          )
        } else {
          Nil
        }
      }
    )

    val reduceRDD: RDD[(String, (Int, Int, Int))] = mapRDD.reduceByKey(
      (a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3)
    )
    reduceRDD.sortBy(_._2,false).take(10).foreach(println)

    sc.stop()
  }
}
