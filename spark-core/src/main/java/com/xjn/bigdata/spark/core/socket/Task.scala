package com.xjn.bigdata.spark.core.socket

/**
 * @author shkstart
 * @create 2021-06-28 19:04
 */
class Task extends Serializable {

  val data = List(1,2,3,4)

  val logic: Int => Int = _ * 2

  def compute() = {
    data.map(logic)
  }
}
