package com.xjn.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._

/**创建项目需要的表并导入数据
 * @author shkstart
 * @create 2021-07-30 16:56
 */
object Spark05_SparkSQL_Hive {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "xjn")

    //TODO 创建SparkSQL运行环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

    spark.sql("use sparksqlproject")
//
//    spark.sql(
//      """
//        |CREATE TABLE `user_visit_action`(
//        | `date` string,
//        | `user_id` bigint,
//        | `session_id` string,
//        | `page_id` bigint,
//        | `action_time` string,
//        | `search_keyword` string,
//        | `click_category_id` bigint,
//        | `click_product_id` bigint,
//        | `order_category_ids` string,
//        | `order_product_ids` string,
//        | `pay_category_ids` string,
//        | `pay_product_ids` string,
//        | `city_id` bigint)
//        |row format delimited fields terminated by '\t'
//        |""".stripMargin)
//
//    spark.sql(
//      """
//        |load data local inpath 'datas/user_visit_action.txt' into table user_visit_action
//        |""".stripMargin)
//
//    spark.sql(
//      """
//        |CREATE TABLE `product_info`(
//        | `product_id` bigint,
//        | `product_name` string,
//        | `extend_info` string)
//        |row format delimited fields terminated by '\t'
//        |""".stripMargin)
//    spark.sql(
//      """
//        |load data local inpath 'datas/product_info.txt' into table product_info
//        |""".stripMargin)
//
//
//    spark.sql(
//      """
//        |CREATE TABLE `city_info`(
//        | `city_id` bigint,
//        | `city_name` string,
//        | `area` string)
//        |row format delimited fields terminated by '\t'
//        |""".stripMargin)
//    spark.sql(
//      """
//        |load data local inpath 'datas/city_info.txt' into table city_info
//        |""".stripMargin)

    spark.sql("select * from product_info").show

    //TODO 关闭运行环境
    spark.close()
  }
}
