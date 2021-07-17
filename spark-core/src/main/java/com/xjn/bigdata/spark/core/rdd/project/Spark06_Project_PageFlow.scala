package com.xjn.bigdata.spark.core.rdd.project

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart
 * @create 2021-07-16 10:23
 * 需求三：页面单跳转换率统计
 */
object Spark06_Project_PageFlow {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("Project1")
    val sc = new SparkContext(sparkConf)
    //1.读取原始日志数据
    val rdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")

    val needpageID: List[Long] = List(1, 2, 3, 4, 5, 6, 7)
    val legelpage: List[(Long, Long)] = needpageID.zip(needpageID.tail)


    val actionRDD: RDD[UserVisitAction] = rdd.map(
      str => {
        val split = str.split("_")
        UserVisitAction(
          split(0),
          split(1).toLong,
          split(2),
          split(3).toLong,
          split(4),
          split(5),
          split(6).toLong,
          split(7).toLong,
          split(8),
          split(9),
          split(10),
          split(11),
          split(12).toLong
        )
      }
    )
    actionRDD.cache()

    //TODO 计算分母
    val reduceRDD: RDD[(Long, Long)] = actionRDD.filter(
      data => {
        needpageID.init.contains(data.page_id)
      }
    ).map(
      action => {
        (action.page_id, 1L)
      }
    ).reduceByKey(_ + _)
    val pageId: Map[Long, Long] = reduceRDD.collect().toMap

    //TODO 计算分子
    val groupRDD: RDD[(String, Iterable[UserVisitAction])] = actionRDD.groupBy(_.session_id)
    val mapValueRDD: RDD[(String, List[((Long, Long), Int)])] = groupRDD.mapValues(
      iter => {
        val sortList: List[UserVisitAction] = iter.toList.sortBy(_.action_time)
        val flowIds: List[Long] = sortList.map(_.page_id)
        val pageflows: List[(Long, Long)] = flowIds.zip(flowIds.tail)

        pageflows.filter(
          t => legelpage.contains(t)
        ).map(
          touple => (touple, 1)
        )
      }
    )
    val result: Array[((Long, Long), Int)] = mapValueRDD.map(_._2).flatMap(list => list).reduceByKey(_ + _).collect()
    //TODO 计算单跳转换率
    //分子除分母
    result.foreach{
      case ((a,b),c) => {
        val l = pageId.getOrElse(a, 0L)
        println(s"页面${a}=>${b}的跳转率为" + (c.toDouble/l))
      }
    }

    sc.stop()
  }

  //用户访问动作表
  case class UserVisitAction(
    date: String,//用户点击行为的日期
    user_id: Long,//用户的 ID
    session_id: String,//Session 的 ID
    page_id: Long,//某个页面的 ID
    action_time: String,//动作的时间点
    search_keyword: String,//用户搜索的关键词
    click_category_id: Long,//某一个商品品类的 ID
    click_product_id: Long,//某一个商品的 ID
    order_category_ids: String,//一次订单中所有品类的 ID 集合
    order_product_ids: String,//一次订单中所有商品的 ID 集合
    pay_category_ids: String,//一次支付中所有品类的 ID 集合
    pay_product_ids: String,//一次支付中所有商品的 ID 集合
    city_id: Long//城市 id
  )

}
