package com.lb.mapping

import java.io.{File, PrintWriter}

import com.sun.deploy.util.StringUtils

import scala.io.Source

/**
  * Created by samsung on 2017/5/2.
  */
object DistinctCrmData {

  def etl(path: String, args: Int *) ={

    val writer = new PrintWriter(new File("E:\\公司资料\\数据映射\\lb-crm-distince.txt"))

    Source.fromFile(new File(path), "UTF-8").getLines().filter { x => x.replaceAll("\"", "").split("\t", -1)(1).size == 11 }.foreach {
      line =>
        //println(line.split("\t", -1)(1))
        writer.println(line.replaceAll("\"", ""))
    }
    writer.close()
  }


  def main(args: Array[String]): Unit ={
    etl("E:\\公司资料\\数据映射\\lb-crm.txt", 0,1,2)
   // println((etl("test", 0) & etl("20170425", 0,1,2,4)).size)
    // println(etl("20170425", 0,1,2).size)
  }

}
