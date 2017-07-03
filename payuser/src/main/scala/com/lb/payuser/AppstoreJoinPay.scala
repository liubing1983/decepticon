package com.lb.payuser

import java.io.File

import scala.io.Source

/**
  * Created by samsung on 2017/4/27.
  */
object AppstoreJoinPay {

  def etl(path: String, args: Int *) : Set[String]={
    val a =  for (file <- (new File(s"E:\\rtdata\\${path}")).listFiles) yield {
        Source.fromFile(file, "UTF-8").getLines().map { line =>
        val lines = line.split("\t", -1)
        ((for(i <- args) yield{ lines(i) }).mkString(","))
      }.toSet
    }
    println(a.flatMap(x => x).size)
    println(a.flatMap(x => x).toSeq.distinct.toSet.size)
    a.flatMap(x => x).toSeq.distinct.toSet
  }


  def main(args: Array[String]): Unit ={
    //etl("20170425", 0,1,2).foreach(println _)
    println((etl("test", 0) & etl("20170425", 0,1,2,4)).size)
    // println(etl("20170425", 0,1,2).size)
  }

}
