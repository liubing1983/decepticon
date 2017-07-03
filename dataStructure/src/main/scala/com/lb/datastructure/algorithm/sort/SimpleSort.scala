package com.lb.datastructure.algorithm.sort

import scala.collection.mutable.ListBuffer

/**
  * Created by liub on 2017/1/19.
  */
object SimpleSort {

  val  a = ListBuffer("3","6","2","7","9","1","8","4","5","0")

  def simple(): Unit ={

    for(i <- 0 until a.size) {
      var q = i
      for (j <- i + 1 until a.size){  q = if (a(q) > a(j)) j else q }
      if(q != i){
        val temp = a(q)
        a(q) = a(i)
        a(i) = temp
      }
      println(a)
    }
    println(a)
  }

  def toForeach[T](f :ListBuffer[T] ): Unit ={
    f.zipWithIndex.foreach{
      case (x, count) => println(s"$x - $count")
    }
  }

  def main(args: Array[String]): Unit ={
   // toForeach(a)
    simple
   // toForeach(a)
  }

}
