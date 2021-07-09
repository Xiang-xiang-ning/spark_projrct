package com.xjn.bigdata.spark.core.socket

import java.io.ObjectInputStream
import java.net.ServerSocket

/**
 * @author shkstart
 * @create 2021-06-28 17:13
 */
object Executor2 {
  def main(args: Array[String]): Unit = {
    //启动服务器,接收数据
    val server = new ServerSocket(8888)
    println("服务器启动，等待接收数据")
    val socket = server.accept()
    val in = socket.getInputStream
    val objIn = new ObjectInputStream(in)
    val task: SubTask = objIn.readObject().asInstanceOf[SubTask]
    val list = task.compute()
    println("计算的结果8888"+list)
    objIn.close()
    socket.close()
    server.close()
  }
}
