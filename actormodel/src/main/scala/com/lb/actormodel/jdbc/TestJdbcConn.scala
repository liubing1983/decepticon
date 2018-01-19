package com.lb.actormodel.jdbc

import java.sql.Connection

import akka.actor._
import akka.routing.RoundRobinPool

/**
  * 说明: 
  * Created by LiuBing on 2018/1/19.
  */

trait JdbcConn

case class MysqlConn(val conn: ConnectionInfo) extends JdbcConn

case class MysqlConn2(val conn: Connection, val i: Int) extends JdbcConn

case class OracleConn() extends JdbcConn

class TestJdbcConn extends Actor with ActorLogging {



  override def receive: Receive = {
    case MysqlConn2(conn, i) => {

      val sql = "select * from search_keyWord_201801 where  report_date = '2018-01-16' limit 1"
      val ps = conn.createStatement()
      //Thread.sleep(1000 * 10)

      val result = ps.executeQuery(sql)
      while (result.next()) {
        println(result.getInt(1) + "--" + i)
      }
context.stop(self)
    }
  }

}

object TestJdbcConn extends App {
  val system = ActorSystem.create("testConn")
  val sysRef = system.actorOf(Props[TestJdbcConn].withRouter(new RoundRobinPool(50)), "123")

  val connInfo = new ConnectionInfo
  val conn = connInfo.getConn


  (0 to 100) map { x =>
    sysRef ! MysqlConn2(conn, x)
  }

  (0 to 1) map { x =>
    connInfo.getStatus()
    Thread.sleep(1000 * 1)
  }
  println("12313123131313123")
  system.wait()
  println(system.terminate())
}
