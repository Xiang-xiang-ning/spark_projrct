package com.xjn.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2021-06-29 20:55
 */
object Rdd_File {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(conf)
    //TODO 创建RDD
    //从文件中创建RDD，将文件中的数据作为处理的数据源
    //path路径默认以当前环境的根路径为基准.可以写绝对路径和相对路径
    //sc.textFile("D:\\IDEAworkspace\\spark_projrct\\datas\\1.txt")
    //val rdd: RDD[String] = sc.textFile("datas/2.txt")
    //path路径可以是文件的具体路径，也可以是目录名称
    val rdd: RDD[String] = sc.textFile("datas/")
    //path路径还可以使用通配符*
    //val rdd = sc.textFile("datas/1*.txt")
    //path路径可以是hdfs上的路径
    //val rdd = sc.textFile("hdfs://hadoop102:8020/test.txt")
    rdd.collect().foreach(println)
    //关闭环境
    sc.stop()
  }
}
