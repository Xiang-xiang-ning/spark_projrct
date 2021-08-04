package com.xjn.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random


/**自定义采集器
 * @author shkstart
 * @create 2021-08-04 10:26
 */
object SparkStreaming02_DIYDataCollect {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val streamContext = new StreamingContext(conf, Seconds(3))

    val messageDS: ReceiverInputDStream[String] = streamContext.receiverStream(new MyReceiver)
    messageDS.print()

    streamContext.start()
    streamContext.awaitTermination()
  }

  //自定义的数据采集器
  class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY){
    var flag = true;
    override def onStart(): Unit = {
      new Thread(
        new Runnable {
          override def run(): Unit = {
            while (flag) {
              val message = "采集的数据为：" + new Random().nextInt(10).toString
              store(message)
              Thread.sleep(500)
            }
          }
        }
      ).start()
    }

    override def onStop(): Unit = {
      flag = false
    }
  }

}
