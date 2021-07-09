package com.xjn.bigdata.spark.core.socket

import java.io.{ObjectOutputStream, OutputStream}
import java.net.Socket

/**
 * @author shkstart
 * @create 2021-06-28 17:12
 */
object Driver {
  def main(args: Array[String]): Unit = {
    //连接服务器，发送数据
    val client1 = new Socket("localhost", 9999)
    val client2 = new Socket("localhost", 8888)
    val task = new Task

    val opt1: OutputStream = client1.getOutputStream
    val objOps1 = new ObjectOutputStream(opt1)
    val subTask1 = new SubTask
    subTask1.data = task.data.take(2)
    subTask1.logic = task.logic
    objOps1.writeObject(subTask1)
    objOps1.flush()
    objOps1.close()
    client1.close()

    val opt2: OutputStream = client2.getOutputStream
    val objOps2 = new ObjectOutputStream(opt2)
    val subTask2 = new SubTask
    subTask2.data = task.data.takeRight(2)
    subTask2.logic = task.logic
    objOps2.writeObject(subTask2)
    objOps2.flush()
    objOps2.close()
    client2.close()
  }
}
