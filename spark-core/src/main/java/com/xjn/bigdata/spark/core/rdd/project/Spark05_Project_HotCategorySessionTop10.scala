package com.xjn.bigdata.spark.core.rdd.project

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2021-07-16 10:23
 * 需求二：Top10 热门品类中每个品类的 Top10 活跃 Session 统计
 */
object Spark05_Project_HotCategorySessionTop10 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("Project1")
    val sc = new SparkContext(sparkConf)

    //1.读取原始日志数据
    val rdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    rdd.cache()
    //将前一个需求求Top10品类封装成一个函数
    val Top10: Array[String] = Top10Categroy(rdd)
    //将不是点击和非Top10品类的数据过滤掉
    val filterRDD: RDD[String] = rdd.filter(
      str => {
        val split = str.split("_")
        if (split(6) != "-1") {
          Top10.contains(split(6))
        } else {
          false
        }
      }
    )
    //((品类，session)，1)
    val mapRDD: RDD[((String, String), Int)] = filterRDD.map(
      str => {
        val split = str.split("_")
        ((split(6), split(2)), 1)
      }
    )
    //ruduce聚合
    val reduceRDD: RDD[((String, String), Int)] = mapRDD.reduceByKey(_ + _)
    //因为要按品类分组，要先转换为(品类，(session，数量))
    val gruRDD: RDD[(String, (String, Int))] = reduceRDD.map(
      touple => (touple._1._1, (touple._1._2, touple._2))
    )
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = gruRDD.groupByKey()
    //对每个品类的session进行降序排序取前10
    val sortRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      touple => {
        touple.toList.sortBy(_._2)(Ordering[Int].reverse).take(10)
      }
    )
    sortRDD.collect().foreach(println)
    sc.stop()
  }

  def Top10Categroy(rdd:RDD[String]): Array[String] ={
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
    val sort: Array[(String, (Int, Int, Int))] = reduceRDD.sortBy(_._2, false).take(10)
    sort.map(_._1)
  }

}
