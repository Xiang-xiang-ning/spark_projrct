package com.xjn.bigdata.spark.core.rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2021-06-30 21:58
 */
object Spark06_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1, 2, 3, 4))
    val user = new User()
    rdd.foreach(
      num => {
        println("年龄是:" + (user.age + num))
      }
    )
    sc.stop()
  }

  class User extends Serializable {
    var age:Int = 22;
  }
}
