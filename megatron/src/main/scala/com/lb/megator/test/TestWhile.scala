package com.lb.megator.test

/**
  * Created by liub on 2016/12/29.
  */
object TestWhile {

  def main(args: Array[String]): Unit ={

println("1111111111111111111")

    val a = (1 to 10000).map{ x =>
      // val a = x
      //a
      (x, 1L)
    }.groupBy{
      case (x, y) =>  1
    }
println(a)
  }

}
