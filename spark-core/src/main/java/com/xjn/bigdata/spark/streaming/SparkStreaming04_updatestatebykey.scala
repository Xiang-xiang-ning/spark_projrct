package com.xjn.bigdata.spark.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**自定义采集器
 *
 * @author shkstart
 * @create 2021-08-04 10:26
 */
object SparkStreaming04_updatestatebykey {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val streamContext = new StreamingContext(conf, Seconds(3))

    streamContext.checkpoint("check")

    val datastream: ReceiverInputDStream[String] = streamContext.socketTextStream("localhost", 9999)
    
    //此处是无状态数据操作，只对当前的采集周期内的数据进行处理，因为reduceByKey
    //datastream.map((_,1)).reduceByKey(_+_).print()

    //换成updatestatusbykey就是状态操作(跨批次)
    //updatestatusbykey:根据key对数据的状态进行更新
    //传递的参数中含有两个值
    //第一个值表示相同的key的value数据
    //第二个值表示缓冲区相同key的value数据
    datastream.map((_,1)).updateStateByKey(
      (seq:Seq[Int],buffer:Option[Int]) => {
        val newCnt = buffer.getOrElse(0)+seq.sum
        Option(newCnt)
      }
    ).print()

    streamContext.start()
    streamContext.awaitTermination()
  }
}
