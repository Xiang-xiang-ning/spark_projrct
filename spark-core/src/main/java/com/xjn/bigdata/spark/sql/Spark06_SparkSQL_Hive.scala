package com.xjn.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**完成项目
 *
 * @author shkstart
 * @create 2021-07-30 16:56
 */
object Spark06_SparkSQL_Hive {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "xjn")

    //TODO 创建SparkSQL运行环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

    spark.sql("use sparksqlproject")

    //查询基本数据
    spark.sql(
      """
        |select u.*,p.product_name,c.city_name,c.area
        |from user_visit_action u
        |join product_info p on u.click_product_id = p.product_id
        |join city_info c on c.city_id = u.city_id
        |where u.click_product_id > -1
        |""".stripMargin).createOrReplaceTempView("t1")

    spark.udf.register("cityRemark",functions.udaf(new cityRemark()))

    //根据区域，商品进行数据聚合
    spark.sql(
      """
        |select area,product_name,count(*) as clickCnt,cityRemark(city_name) as city_remark
        |from t1
        |group by area,product_name
        |""".stripMargin).createOrReplaceTempView("t2")

    //区域内对点击数进行排行
    spark.sql(
      """
        |select * ,rank() over(partition by area order by clickCnt desc ) rank
        |from t2
        |""".stripMargin).createOrReplaceTempView("t3")

    //取排行前三的
    spark.sql(
      """
        |select *
        |from t3
        |where rank<=3
        |""".stripMargin).show(false)

    //TODO 关闭运行环境
    spark.close()
  }

  case class Buff(var total:Long,var cityMap:mutable.Map[String,Long])

  class cityRemark extends Aggregator[String,Buff,String] {
    //缓冲区初始化
    override def zero: Buff = {
      Buff(0,mutable.Map[String,Long]())
    }

    override def reduce(buff: Buff, city: String): Buff = {
      buff.total += 1
      val cnt = buff.cityMap.getOrElse(city,0L)+1
      buff.cityMap.update(city,cnt)
      buff
    }

    //分布式中的所有buffer合并成一个
    override def merge(buff1: Buff, buff2: Buff): Buff = {
      buff1.total+=buff2.total
      buff1.cityMap.foreach(
        touple => {
          val newCnt = buff2.cityMap.getOrElse(touple._1,0L) + touple._2
          buff2.cityMap.update(touple._1,newCnt)
        }
      )
      buff2
    }

    override def finish(result: Buff): String = {
      val remarkList: ListBuffer[String] = ListBuffer[String]()
      val totalcnt = result.total
      val map = result.cityMap
      val cityCntList: List[(String, Long)] = map.toList.sortWith(
        (left, right) => {
          left._2 > right._2
        }
      ).take(2)
      val havemore = map.size > 2
      var totalrate = 0L
      cityCntList.foreach{
        case (city,cnt) => {
          var rate = cnt*100/totalcnt
          totalrate+=rate
            remarkList.append(s"${city} ${rate}%")
        }
      }
      if(havemore){
        remarkList.append(s"其他 ${100-totalrate}%")
      }

      remarkList.mkString(",")

    }

    override def bufferEncoder: Encoder[Buff] = Encoders.product

    override def outputEncoder: Encoder[String] = Encoders.STRING
  }

}
