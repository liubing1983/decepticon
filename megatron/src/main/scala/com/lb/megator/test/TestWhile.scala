package com.lb.megator.test

/**
  * Created by liub on 2016/12/29.
  */
case class Abc(var a: String)

object TestWhile {

  val h = Abc("c")

  def a(i: String): Unit = {
    h.a = i
  }

  def b(i: String): Unit = {
    h.a = i
  }

  def main(args: Array[String]): Unit = {

    // 初始值
    println(h.a)
    //第一次调用
    TestWhile.a("a")
    println(h.a)
    // 第二次调用
    TestWhile.b("b")
    println(h.a)

    println("1111111111111111111")
    // abc("a")

    val a = (1 to 10000).map { x =>
      // val a = x
      //a
      (x, 1L)
    }.groupBy {
      case (x, y) => 1
    }
    println(a)
  }
}