package com.lb.log4jtest.error

import com.lb.log4jtest.debug.TestA
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by samsung on 2017/4/11.
  */
object TestC {

  val logger: Logger = LoggerFactory.getLogger(TestC.getClass)

  def main(args: Array[String]): Unit = {
    logger.debug("C ---  debug")
    logger.info("C ---  info")
    logger.error("C ---  error")
  }

}
