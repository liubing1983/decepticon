package com.lb.log4jtest.test


import com.lb.log4jtest.debug.TestA
import com.lb.log4jtest.error.TestC
import com.lb.log4jtest.info.TestB
import org.slf4j.{Logger, LoggerFactory}

/**
  * 测试为不同的包配置不同的输出路径和log 级别
  * Created by samsung on 2017/4/11.
  */
object TestTest {

  val logger: Logger = LoggerFactory.getLogger(TestTest.getClass)

  def main(args: Array[String]): Unit = {
    while (true) {
    logger.debug("other ---  debug")
    logger.info("other ---  info")
    logger.error("other ---  error")
      try
        Thread.sleep(30)
      catch {
        case e: InterruptedException =>
          e.printStackTrace()
      }
  }
    TestA.main(Array())
    TestB.main(Array())
    TestC.main(Array())

    val a = new HelloWord
    a.hw("log4j")
  }
}
