package com.lb.log4jtest.test

import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by samsung on 2017/4/11.
  */
class HelloWord {
  val logger: Logger = LoggerFactory.getLogger(TestTest.getClass)

  def hw(s: String): Unit ={
    logger.debug(s"Hello, ${s} ---  debug")
    logger.info(s"Hello, ${s} ---  info")
    logger.error(s"Hello, ${s} ---  error")
  }

}
