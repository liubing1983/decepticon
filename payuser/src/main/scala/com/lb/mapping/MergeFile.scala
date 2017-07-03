package com.lb.mapping

import java.io.{File, PrintWriter}

import scala.io.Source

/**
  * Created by samsung on 2017/5/3.
  */
object MergeFile {

  /**
    * 合并spark计算完成后的数据
    */
  def merge(): Unit ={
    val writer = new PrintWriter(new File("E:\\公司资料\\数据映射\\mapping.txt"))
    for (file <- (new File("E:\\公司资料\\数据映射\\lb")).listFiles) {
      Source.fromFile(file, "UTF-8").getLines() .foreach {
        writer.println(_)
      }
    }
    writer.close()
  }

  /**
    * 过滤北京和保定的数据
    */
  def filterCity(): Unit ={
    val writer = new PrintWriter(new File("E:\\公司资料\\数据映射\\mapping_city.txt"))

      Source.fromFile("E:\\公司资料\\数据映射\\mapping.txt", "UTF-8").getLines().filter{t => val x =t.split("\t", -1); x(41).indexOf("保定") >= 0 || x(41).indexOf("北京") >=0|| x(50).indexOf("保定") >=0|| x(50).indexOf("北京") >=0 }.foreach {
        writer.println(_)
      }

    writer.close()
  }


  def main(args: Array[String]): Unit ={
    filterCity
  }

}
