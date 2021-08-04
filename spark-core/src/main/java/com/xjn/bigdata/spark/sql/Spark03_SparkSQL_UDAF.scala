package com.xjn.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @author shkstart
 * @create 2021-07-30 16:56
 */
object Spark03_SparkSQL_UDAF {
  def main(args: Array[String]): Unit = {
    //TODO 创建SparkSQL运行环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = session.read.json("datas/user.json")
    df.createOrReplaceTempView("user")

    //自定义函数UDF
    session.udf.register("avgUDAF",new MyUDAF)

    session.sql("select avgUDAF(age) from user").show()

    //TODO 关闭运行环境
    session.close()
  }

  //自定义聚合函数类：计算年龄的平均值
  //1.继承UserDefinedAggregateFunction
  //2.重写8个方法
  class MyUDAF extends UserDefinedAggregateFunction {
    //输入数据的结构
    override def inputSchema: StructType = {
      StructType(Array(StructField("age",LongType)))
    }
    //缓冲区数据的结构
    override def bufferSchema: StructType = {
      StructType(Array(StructField("total",LongType),StructField("count",LongType)))
    }
    //输出数据的结构
    override def dataType: DataType = LongType

    override def deterministic: Boolean = true

    //缓冲区初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0,0L)
      buffer.update(1,0L)
    }

    //根据输入的值更新缓冲区数据
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer.update(0,input.getLong(0)+buffer.getLong(0))
      buffer.update(1,buffer.getLong(1)+1)
    }

    //缓冲区数据合并(因为分布式计算buffer肯定有多个)
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0,buffer1.getLong(0)+buffer2.getLong(0))
      buffer1.update(1,buffer1.getLong(1)+buffer2.getLong(1))
    }

    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0)/buffer.getLong(1)
    }
  }

}
