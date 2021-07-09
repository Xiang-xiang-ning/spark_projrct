package com.xjn.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2021-06-30 21:58
 * sortBy算子
 */
object Spark11_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    //TODO 算子：双value类型
    val rdd1 = sc.makeRDD(List(1, 2, 3, 4),2)
    val rdd2 = sc.makeRDD(List(3, 4, 5, 6),2)
    //交集并集差集要求两个数据源的数据类型相同
    //交集 [3,4]  ps:mkString输入是一行显示，而foreach是多行
    //rdd1.intersection(rdd2).collect().foreach(println)

    //并集[1,2,3,4,3,4,5,6]
    //println(rdd1.union(rdd2).collect().mkString(","))

    //差集如果rdd1在前是[1,2]，反之是[5,6]
    //println(rdd1.subtract(rdd2).collect().mkString(","))

    //拉链 [1-3,2-4,3-5,4-6]
    //Can't zip RDDs with unequal numbers of partitions
    //两个数据源要求分区数量保持一致
    //Can only zip RDDs with same number of elements in each partition
    //两个数据源要求分区中数据的数量保持一致
    println(rdd1.zip(rdd2).collect().mkString(","))
    sc.stop()
  }
}
