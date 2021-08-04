package com.xjn.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @author shkstart
 * @create 2021-07-30 16:56
 */
object Spark01_SparkSQL_Basic {
  def main(args: Array[String]): Unit = {
    //TODO 创建SparkSQL运行环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import session.implicits._

    //TODO 业务逻辑
    //生成DataFrame
    val frame: DataFrame = session.read.json("datas/user.json")
    //frame.show()

    //DataFrame=>sql
    //frame.createOrReplaceTempView("user")
    //session.sql("select * from user").show()
    //session.sql("select age from user").show()
    //session.sql("select age+1 from user").show()

    //DataFrame=>DSL
    //如果要改变列的值要导入隐式转换
    //frame.select("username","age").show()
    //frame.select('username, 'age+1).show()

    //DataSet
    //val list = List(1, 2, 3, 4)
    //list.toDS().show()

    //RDD<=>DataFrame
    val rdd: RDD[(Int, String, Int)] = session.sparkContext.makeRDD(List((1, "xjn", 23), (2, "wsy", 26)))
    rdd.collect().foreach(println)
    val df: DataFrame = rdd.toDF("id", "username", "age")
    df.show()
    val rdd1: RDD[Row] = df.rdd
    rdd1.collect().foreach(println)

    //DataFrame <=> DataSet
    val ds: Dataset[User] = df.as[User]
    ds.show()
    val df1: DataFrame = ds.toDF()
    df1.show()
    println("===========")

    //RDD <=> DataSet
    val ds1: Dataset[User] = rdd.map(
      touple => {
        new User(touple._1, touple._2, touple._3)
      }
    ).toDS()
    ds1.show()
    val rdd2: RDD[User] = ds1.rdd
    rdd2.collect().foreach(println)
    //TODO 关闭运行环境
    session.close()
  }

  case class User(id:Int,username:String,age:Int)

}
