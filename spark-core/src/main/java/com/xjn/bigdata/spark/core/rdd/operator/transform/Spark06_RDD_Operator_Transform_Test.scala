package com.xjn.bigdata.spark.core.rdd.operator.transform

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * @author shkstart
 * @create 2021-06-30 21:58
 * Test:计算apache.log中各个时间段的点击次数
 */
object Spark06_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[String] = sc.textFile("datas/apache.log")
    val HourRDD = rdd.map(
      data => {
        val split = data.split(" ")
        val time: String = split(3)
        val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val date = sdf.parse(time)
        val sdf1 = new SimpleDateFormat("HH")
        val hour: String = sdf1.format(date)
        hour
      }
    )
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = HourRDD.map(
      data => {
        (data, 1)
      }
    ).groupBy(_._1)

    groupRDD.map(
      touple => {(touple._1,touple._2.size)}
    ).collect().foreach(println)

//    groupRDD.map {
//      case (hour, iter) => {
//        (hour, iter.size)
//      }
//    }.collect().foreach(println)
//    HourRDD.map(
//      data => (data,1)
//    ).reduceByKey(_+_).collect().foreach(println)
    sc.stop()
  }
}
