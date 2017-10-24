package com.lb.book.scala_cookbook.p04

/**
  * 说明: 4.4 定义私有主构造函数
  * Created by LiuBing on 2017/10/12.
  */
class Order private {
  println("1213123")
}

object Order {
  val o = new Order

  def oo = o
}


