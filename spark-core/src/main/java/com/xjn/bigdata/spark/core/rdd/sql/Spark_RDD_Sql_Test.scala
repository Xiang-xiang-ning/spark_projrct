package com.xjn.bigdata.spark.core.rdd.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2021-06-30 21:58
 * 统计出每一个省份每个广告被点击数量排行的 Top3
 * agent.log对应的数据顺序为时间戳，省份，城市，用户，广告
 *
 */
object Spark_RDD_Sql_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    //1.获取原始数据：时间戳，省份，城市，用户，广告
    val rdd: RDD[String] = sc.textFile("datas/agent.log")
    //2.将原始数据进行结构的转换。方便统计
    //=>((省份，广告),1)
    val splitRDD: RDD[((String, String), Int)] = rdd.map(
      str => {
        val split = str.split(" ")
        ((split(1), split(4)), 1)
      }
    )
    //3.将转换结构后的数据，进行分组聚合
    //((省份，广告),1) => ((省份，广告),sum)
    val reduceRDD: RDD[((String, String), Int)] = splitRDD.reduceByKey(_ + _)
    //4.将聚合的结果进行结构的转换
    //((省份，广告),sum)=>(省份，(广告,sum))
    val mapRDD = reduceRDD.map(
      touple => {
        (touple._1._1, (touple._1._2, touple._2))
      }
    )
    //5.将转换结构后的数据根据省份进行分组
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupByKey()
    //6.将分组后的数据组内排序(降序)，取前三名
    val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      }
    )
    //7.采集数据并打印在控制台
    resultRDD.collect().foreach(println)
    sc.stop()
  }
}
