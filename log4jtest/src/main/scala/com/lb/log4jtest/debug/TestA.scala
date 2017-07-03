package com.lb.log4jtest.debug

import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by samsung on 2017/4/11.
  */
object TestA {

  val logger: Logger = LoggerFactory.getLogger(TestA.getClass)

  def main(args: Array[String]): Unit = {
    logger.debug("A ---  debug")
    logger.info("A ---  info")
    logger.error("A ---  error")
  }

}
