package com.xjn.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

/**
 * @author shkstart
 * @create 2021-07-30 16:56
 */
object Spark04_SparkSQL_JDBC {
  def main(args: Array[String]): Unit = {
    //TODO 创建SparkSQL运行环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df = session.read.format("jdbc")
      .option("url", "jdbc:mysql://hadoop102:3306/spark-sql")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "000000")
      .option("dbtable", "user")
      .load()
    df.show()

    df.write.format("jdbc")
      .option("url", "jdbc:mysql://hadoop102:3306/spark-sql")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "000000")
      .option("dbtable", "user1")
      .mode(SaveMode.Append)
      .save()

    //TODO 关闭运行环境
    session.close()
  }
}
