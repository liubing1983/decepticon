package com.lb.datastructure.algorithm.sort

import scala.collection.mutable.ListBuffer

/**
  *  直接插入排序
  * Created by liub on 2017/1/19.
  */
object StraightSort {

  val  a = ListBuffer("3","6","2","7","9","1","8","4","5","0")

  def  straight(): Unit ={
    var x = 0
    for(i <-  1 until a.size){
          if(a(i-1) < a(i)){
            a(x) = a(i)
            for(j <- i-1 until 0 ){

            }
          }
    }
  }

}
