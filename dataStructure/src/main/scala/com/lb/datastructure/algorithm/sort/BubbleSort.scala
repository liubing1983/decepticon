package com.lb.datastructure.algorithm.sort

/**
  *  冒泡排序
  * Created by liub on 2017/1/18.
  */
object BubbleSort {

  val  a = scala.collection.mutable.ListBuffer("3","6","2","7","9","1","8","4","5","0")

  def bubble1(): Unit ={
    for(i <- 0 until a.size ; j <- 0 until a.size -i -1){
        if(a(j) < a(j + 1)){
          val temp = a(j)
          a(j) = a(j+1)
          a(j+1) = temp
      }
    }
    println(a)
  }

  def main(args : Array[String]): Unit ={
    bubble1
  }
}