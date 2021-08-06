package com.xjn.bigdata.spark.streaming

import java.sql.{Connection, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date

import com.xjn.bigdata.spark.util.JDBCUtil
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer


/**
 *
 * @author shkstart
 * @create 2021-08-04 10:26
 */
object SparkStreamingProject01_BlackList {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val streamContext = new StreamingContext(conf, Seconds(3))


    //定义 Kafka 参数
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG ->
        "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "atguiguNew",
      "key.deserializer" ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" ->
        "org.apache.kafka.common.serialization.StringDeserializer"
    )
    //读取 Kafka 数据创建 DStream
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream[String, String](streamContext,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Set("atguiguNew"), kafkaPara))

    //取出DStream中的value值
    val ClickData: DStream[AdClickData] = kafkaDStream.map(
      kafkadata => {
        val data = kafkadata.value()
        val datalist = data.split(" ")
        AdClickData(datalist(0), datalist(1), datalist(2), datalist(3), datalist(4))
      }
    )

    //TODO 周期性获取黑名单数据
    val ds: DStream[((String, String, String), Int)] = ClickData.transform(
      rdd => {
        val blacklist = ListBuffer[String]()
        val conn1: Connection = JDBCUtil.getConnection
        val stat = conn1.prepareStatement("select userid from black_list")
        val result: ResultSet = stat.executeQuery()
        while (result.next()) {
          blacklist.append(result.getString(1))
        }
        result.close()
        stat.close()
        conn1.close()

        val filterRDD: RDD[AdClickData] = rdd.filter(
          data => {
            //TODO 判断用户是否在黑名单中
            !blacklist.contains(data.userid)
          }
        )
        //TODO 如果用户不在黑名单中，那么进行统计数量(每个采集周期)
        filterRDD.map(
          datas => {
            val sdf = new SimpleDateFormat("yyyy-MM-dd")
            val date = sdf.format(new Date(datas.ts.toLong))
            val userid = datas.userid
            val adid = datas.adid
            ((date, userid, adid), 1)
          }
        ).reduceByKey(_ + _)
      }
    )

    //RDD.foreach会为每条数据创建一个连接
    //foreach是RDD算子，算子内部在executor执行，算子外部在driver端执行
    //这样就会涉及闭包操作，需要将driver端的连接发送到executor端，这就需要进行序列化
    //数据库的连接对象是不能序列化的

    //RDD提供了一个算子可以有效提高效率：foreachPartition
    //可以一个分区创建一个连接对象，大幅度减少连接数
//    ds.foreachRDD(
//      rdd => {
//        rdd.foreachPartition(
//          iterator => {
//            val conn = JDBCUtil.getConnection
//            iterator.foreach(
//              .......
//            )
//          }
//        )
//      }
//    )

    //TODO 如果更新后的点击数超过阈值，则加入黑名单
    ds.foreachRDD(
      rdd => {
        rdd.foreach{
          case ((date, uid, aid), cnt) => {
            println(s"${date} ${uid} ${aid} ${cnt}")
            if (cnt >= 30) {
              //TODO 如果统计数量超过阈值(30)，则将用户加入黑名单
              val conn2 = JDBCUtil.getConnection
              val sql1 = """
                           |insert ignore into black_list (userid) values(?)
                           |""".stripMargin
              JDBCUtil.executeUpdate(conn2,sql1,Array(uid))
//              val stat = conn.prepareStatement(
//                """
//                  |insert ignore into black_list (userid) values(?)
//                  |""".stripMargin)
//              stat.setString(1,uid)
//              stat.executeUpdate()
//              stat.close()
              conn2.close()
            }else {
              //TODO 如果没有超过阈值，则将今日的点击数进行更新
              //查询统计表数据
              val conn3 = JDBCUtil.getConnection
              val sql2 = """
                           |select *
                           |from user_ad_count
                           |where dt = ? and userid = ? and adid = ?
                           |""".stripMargin
              val flag1 = JDBCUtil.isExist(conn3, sql2, Array(date, uid, aid))
//              val st = conn1.prepareStatement(
//                """
//                  |select *
//                  |from user_ad_count
//                  |where dt = ? and userid = ? and adid = ?
//                  |""".stripMargin)
//              st.setString(1,date)
//              st.setString(2,uid)
//              st.setString(3,aid)
//              val result = st.executeQuery()


              if (flag1){
                //如果存在就累加
                val sql3 = """
                             |update user_ad_count
                             |SET count = count + ?
                             |where dt = ? and adid = ? and userid = ?
                             |""".stripMargin
                JDBCUtil.executeUpdate(conn3,sql3,Array(cnt,date,aid,uid))
//                val stat = conn1.prepareStatement(
//                  """
//                    |update user_ad_count
//                    |SET count = count + ?
//                    |where dt = ? and adid = ? and userid = ?
//                    |""".stripMargin)
//                stat.setInt(1,cnt)
//                stat.setString(2,date)
//                stat.setString(3,aid)
//                stat.setString(4,uid)
//                stat.executeUpdate()
//                stat.close()

                val sql4 = """
                             |select *
                             |from user_ad_count
                             |where dt = ? and userid = ? and adid = ? and count >= 30
                             |""".stripMargin
                val flag2 = JDBCUtil.isExist(conn3, sql4, Array(date, uid, aid))
//                val stat2 = conn1.prepareStatement(
//                  """
//                    |select *
//                    |from user_ad_count
//                    |where dt = ? and userid = ? and adid = ? and count >= 30
//                    |""".stripMargin)
//                stat2.setString(1,date)
//                stat2.setString(2,uid)
//                stat2.setString(3,aid)
//                val result1 = stat2.executeQuery()

                //TODO 累加完还要看是否超出阈值
                //如果超出加入黑名单
                if (flag2){
                  val sql4 = """
                               |insert ignore into black_list (userid) values(?)
                               |""".stripMargin
                  JDBCUtil.executeUpdate(conn3,sql4,Array(uid))
//                  val stat3 = conn1.prepareStatement(
//                    """
//                      |insert ignore into black_list (userid) values(?)
//                      |""".stripMargin)
//                  stat3.setString(1,uid)
//                  stat3.executeUpdate()
//                  stat3.close()
                }

              }else{
                //如果不存在就新增
                val sql5 = """
                             |insert into user_ad_count (dt,userid,adid,count) values(?,?,?,?)
                             |""".stripMargin
                JDBCUtil.executeUpdate(conn3,sql5,Array(date,uid,aid,cnt))
//                val stat1 = conn1.prepareStatement(
//                  """
//                    |insert into user_ad_count (dt,userid,adid,count) values(?,?,?,?)
//                    |""".stripMargin)
//                stat1.setString(1,date)
//                stat1.setString(2,uid)
//                stat1.setString(3,aid)
//                stat1.setInt(4,cnt)
//                stat1.executeUpdate()
//                stat1.close()

              }
            conn3.close()
            }
          }
        }
      }
    )

    streamContext.start()
    streamContext.awaitTermination()
  }

  //广告点击数据的样例类
  case class AdClickData(ts:String,area:String,city:String,userid:String,adid:String)
}
