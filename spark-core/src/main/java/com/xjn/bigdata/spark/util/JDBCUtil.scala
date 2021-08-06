package com.xjn.bigdata.spark.util

import java.sql.{Connection, PreparedStatement}
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource

/**
 * @author shkstart
 * @create 2021-08-06 10:36
 */
object JDBCUtil {
  //初始化连接池
  var dataSource: DataSource = init()
  //初始化连接池方法
  def init(): DataSource = {
    val properties = new Properties()
    properties.setProperty("driverClassName", "com.mysql.jdbc.Driver")
    properties.setProperty("url", "jdbc:mysql://hadoop102:3306/spark-streaming")
    properties.setProperty("username", "root")
    properties.setProperty("password", "000000")
    properties.setProperty("maxActive", "50")
    DruidDataSourceFactory.createDataSource(properties)
  }

  //执行 SQL 语句,单条数据插入
  def executeUpdate(connection: Connection, sql: String, params: Array[Any]): Int
  = {
    var rtn = 0
    var pstmt: PreparedStatement = null
    try {
      connection.setAutoCommit(false)
      pstmt = connection.prepareStatement(sql)
      if (params != null && params.length > 0) {
        for (i <- params.indices) {
          pstmt.setObject(i + 1, params(i))
        }
      }
      rtn = pstmt.executeUpdate()
      connection.commit()
      pstmt.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    rtn
  }

  //判断一条数据是否存在
  def isExist(connection: Connection, sql: String, params: Array[Any]): Boolean =
  {
    var flag: Boolean = false
    var pstmt: PreparedStatement = null
    try {
      pstmt = connection.prepareStatement(sql)
      for (i <- params.indices) {
        pstmt.setObject(i + 1, params(i))
      }
      flag = pstmt.executeQuery().next()
      pstmt.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    flag
  }

  //获取 MySQL 连接
  def getConnection: Connection = {
    dataSource.getConnection
  }
}
