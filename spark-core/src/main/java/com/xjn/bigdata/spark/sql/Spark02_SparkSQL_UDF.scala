package com.xjn.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @author shkstart
 * @create 2021-07-30 16:56
 */
object Spark02_SparkSQL_UDF {
  def main(args: Array[String]): Unit = {
    //TODO 创建SparkSQL运行环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import session.implicits._

    val df: DataFrame = session.read.json("datas/user.json")
    df.createOrReplaceTempView("user")

    //自定义函数UDF
    session.udf.register("addName",(name:String) => {
      "Name:"+name
    })

    session.sql("select addName(username),age from user").show()

    //TODO 关闭运行环境
    session.close()
  }

}
