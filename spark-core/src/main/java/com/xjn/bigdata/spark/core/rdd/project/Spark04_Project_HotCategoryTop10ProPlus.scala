package com.xjn.bigdata.spark.core.rdd.project

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author shkstart
 * @create 2021-07-16 10:23
 * 需求一：Top10热门品类
 */
object Spark04_Project_HotCategoryTop10ProPlus {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("Project1")
    val sc = new SparkContext(sparkConf)

    //1.读取原始日志数据
    val rdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")

    val acc: HotCategoryAccumulator = new HotCategoryAccumulator
    sc.register(acc,"MyAcc")


    rdd.map(
      word => {
        val split = word.split("_")
        if (split(6) != "-1") {
          acc.add((split(6),"click"))
        } else if (split(8) != "null") {
          val order = split(8).split(",")
          order.foreach( acc.add(_,"order"))
        } else if (split(10) != "null") {
          val pay = split(10).split(",")
          pay.foreach( acc.add(_,"pay"))
        }
      }
    )

    val result: mutable.Map[String, HotCategory] = acc.value
    val categories: mutable.Iterable[HotCategory] = result.map(_._2)
    val sort: List[HotCategory] = categories.toList.sortWith(
      (left, right) => {
        if (left.clickCnt > right.clickCnt) {
          true
        } else if (left.clickCnt == right.clickCnt) {
          if (left.orderCnt > right.orderCnt){
            true
          } else if (left.orderCnt == right.orderCnt){
            if (left.payCnt > right.payCnt){
              true
            }else {
              false
            }
          }
          else {
            false
          }
        } else {
          false
        }
      }
    )
    sort.take(10).foreach(println)

    sc.stop()
  }

  case class HotCategory(cid:String,var clickCnt:Int,var orderCnt:Int,var payCnt:Int)

  //自定义累加器
  //IN:(品类，行为类型)
  //OUT：mutable.Map[String,HotCategory]
  class HotCategoryAccumulator extends AccumulatorV2[(String,String),mutable.Map[String,HotCategory]]{

    private val HCmap = mutable.Map[String,HotCategory]()

    override def isZero: Boolean = {
      HCmap.isEmpty
    }

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = {
      new HotCategoryAccumulator
    }

    override def reset(): Unit = {
      HCmap.clear()
    }

    override def add(v: (String, String)): Unit = {
      val cid = v._1
      val actionType = v._2
      val category = HCmap.getOrElse(cid, HotCategory(cid, 0, 0, 0))
      if (actionType == "click"){
        category.clickCnt += 1
      }else if (actionType == "order"){
        category.orderCnt += 1
      }else if (actionType == "pay"){
        category.payCnt += 1
      }
      HCmap.update(cid,category)
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
      val map1 = this.HCmap
      val map2 = other.value
      map2.foreach(
        touple => {
          val category = map1.getOrElse(touple._1, HotCategory(touple._1, 0, 0, 0))
          category.clickCnt += touple._2.clickCnt
          category.orderCnt += touple._2.orderCnt
          category.payCnt += touple._2.payCnt
          map1.update(touple._1,category)
        }
      )
    }

    override def value: mutable.Map[String, HotCategory] = {
      HCmap
    }
  }

}