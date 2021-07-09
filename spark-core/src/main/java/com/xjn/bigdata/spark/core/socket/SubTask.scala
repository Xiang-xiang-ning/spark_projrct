package com.xjn.bigdata.spark.core.socket

/**
 * @author shkstart
 * @create 2021-06-29 12:41
 */
class SubTask extends Serializable {
  var data :List[Int] = _
  var logic :Int=>Int = _

  def compute() = {
    data.map(logic)
  }

}
