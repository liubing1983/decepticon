package com.lb.megator.jdbc

import java.sql._
import java.util.Properties

import com.mchange.v2.c3p0.ComboPooledDataSource

/**
  * 说明: 
  * Created by LiuBing on 2018/1/17.
  */
object ConnectionInfo {
  println("123")
  val cpds = new ComboPooledDataSource

  //加载配置文件
  val props = new Properties()
  props.load(this.getClass.getClassLoader.getResourceAsStream("c3p0.properties"))

  cpds.setDriverClass(props.getProperty("DriverClass"))
  cpds.setJdbcUrl(props.getProperty("JdbcUrl"))
  cpds.setUser(props.getProperty("User"))
  cpds.setPassword(props.getProperty("Password"))

  cpds.setMaxPoolSize(props.getProperty("MaxPoolSize").toInt)
  cpds.setMinPoolSize(props.getProperty("MinPoolSize").toInt)
  cpds.setInitialPoolSize(props.getProperty("InitialPoolSize").toInt)
  cpds.setMaxStatements(props.getProperty("MaxStatements").toInt)
  cpds.setMaxIdleTime(props.getProperty("MaxIdleTime").toInt)

  val conn: Connection = null

  def getConn(): Connection = {
    if (conn == null) {
      println("申请Connection连接!!!")
      cpds.getConnectionPoolDataSource().getPooledConnection().getConnection()
    } else {
      println("清闲的：" + cpds.getNumIdleConnections)
      println("忙碌的：" + cpds.getNumBusyConnections)
      println("所有的：" + cpds.getNumConnections)

      conn
    }
  }


  def close(conn: Connection, ps: Statement, rs: ResultSet): Unit = {
    try {
      if (null != rs) rs.close()
      if (null != ps) ps.close
      if (null != conn) conn.close
    } catch {
      case e: SQLException =>
        println("关闭连接时发生错误 An error occurred while closing the connection", e)
    }
  }

}

object ConnTest extends App {


  val sql = "select * from search_keyWord_201801 where  report_date = '2018-01-16' limit 5"
  val connInfo = ConnectionInfo
  val conn = connInfo.getConn()
  val ps = conn.createStatement()

  val result = ps.executeQuery(sql)
  println("2222")
  while (result.next()) {
    println("3333")
    println(result.toString)
  }
  Thread.sleep(1000 * 100)
  connInfo.close(conn, ps , result)
println("4444")
}