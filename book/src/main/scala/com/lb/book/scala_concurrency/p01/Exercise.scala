package com.lb.book.scala_concurrency.p01

/**
  * 说明: 
  * Created by LiuBing on 2018/1/18.
  */
object Exercise {

  def compose[A, B, C](g: B => C, f: A => B): A => C = {
       a => g(f(a))
  }


}
