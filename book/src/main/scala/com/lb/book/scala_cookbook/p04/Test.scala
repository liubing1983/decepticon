package com.lb.book.scala_cookbook.p04

/**
  * 说明:
  * Created by LiuBing on 2017/10/12.
  */
object Test  extends App{

  // 演示调用Order
  // 可以通过伴生对象来调用
  val b = Order.oo

  /**
    * 因为order是private的, 所以调用失败
    * Error:(9, 12) constructor Order in class Order cannot be accessed in object Test
      val a  = new Order()
    */
  //val a  = new Order()



}
