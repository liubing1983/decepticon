package com.lb.log4jtest.info

import com.lb.log4jtest.debug.TestA
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by samsung on 2017/4/11.
  */
object TestB {

  val logger: Logger = LoggerFactory.getLogger(TestB.getClass)

  def main(args: Array[String]): Unit = {
    logger.debug("B ---  debug")
    logger.info("B ---  info")
    logger.error("B ---  error")
  }

}
