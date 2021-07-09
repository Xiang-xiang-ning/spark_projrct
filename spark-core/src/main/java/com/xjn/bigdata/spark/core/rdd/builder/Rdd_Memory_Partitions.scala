package com.xjn.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2021-06-29 20:55
 */
object Rdd_Memory_Partitions {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    conf.set("spark.default.parallelism","5")
    val sc = new SparkContext(conf)
    //TODO 创建RDD
    //makeRDD的第二个参数为分区数量
    //第二个参数可以不传，默认为defaultParallelism
    //默认值为scheduler.conf.getInt("spark.default.parallelism", totalCores)
    //spark在默认情况下，从配置对象sparkConf中获取配置参数：spark.default.parallelism
    //如果获取不到，则使用totalCores，表示当前运行环境最大可用核数
    val rdd = sc.makeRDD(
      List(1, 2, 3, 4)
    )
    rdd.saveAsTextFile("output/")
    //关闭环境
    sc.stop()
  }
}
