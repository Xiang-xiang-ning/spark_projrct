package com.xjn.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row, SparkSession, functions}

/**
 * @author shkstart
 * @create 2021-07-30 16:56
 */
object Spark03_SparkSQL_UDAF_Plus {
  def main(args: Array[String]): Unit = {
    //TODO 创建SparkSQL运行环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = session.read.json("datas/user.json")
    df.createOrReplaceTempView("user")

    //自定义函数UDF
    session.udf.register("ageAvg",functions.udaf(new MyAvgUDAF))

    session.sql("select ageAvg(age) from user").show()

    //TODO 关闭运行环境
    session.close()
  }

  //自定义聚合函数类：计算年龄的平均值
  //查看UserDefinedAggregateFunction源码@scala.deprecated()
  //1.继承org.apache.spark.sql.expressions.Aggregator，定义泛型
  // IN：输入类型  BUF：缓冲区类型  OUT：输出类型
  case class BUF(var total:Long,var count:Long)
  class MyAvgUDAF extends Aggregator[Long,BUF,Long] {
    //初始化缓冲区
    override def zero: BUF = {
      BUF(0L,0L)
    }
    //根据输入的数据更新缓冲区的数据
    override def reduce(buf: BUF, in: Long): BUF = {
      buf.total = buf.total + in
      buf.count = buf.count + 1
      buf
    }
    //合并缓冲区
    override def merge(b1: BUF, b2: BUF): BUF = {
      b1.total = b1.total + b2.total
      b1.count = b1.count + b2.count
      b1
    }

    //计算结果
    override def finish(reduction: BUF): Long = {
      reduction.total / reduction.count
    }

    //缓冲区的编码，自定义类的编码都是这个
    override def bufferEncoder: Encoder[BUF] = Encoders.product

    //输出的编码，根据类型来
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }


}
