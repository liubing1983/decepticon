package com.lb.spark.utils

import com.lb.spark.utils.DateUtils._

/**
  * Created by samsung on 2017/6/27.
  */
object Test {
def main(args: Array[String]): Unit ={

  val a = ""

  val b = ",a,b,c,a,d,"

  println(abc("","")+"-----")

  def abc(s: String, y:String) : String ={
    if(s.size > 0 || y.size > 0) "1" else ""
  }



  //val bb = dups(b.split("," ).toList)

  val seta =  b.split(",").toSet.filter(_ != "")

  println(seta.toString())
  println(seta.mkString)

 // bb.foreach(println _)
  //println(bb.toArray.mkString("="))

  a match  {
    case "" => println("111111111111111")
    case x: String =>  println("222222222222")
  }

  println(a == "")



  val l : Long = 1498411740000L
  val s = l.dateFormat
  println(s)
}

  def dups[T](list: List[T]) = list.foldLeft(List.empty[T]){
    (seen, cur)  =>
      if(seen.contains(cur)  ) (seen) else (seen :+ cur)
  }
}
