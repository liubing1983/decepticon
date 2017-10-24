package com.lb.book.scala_function.p04

/**
  * 说明: 
  * Created by LiuBing on 2017/10/24.
  */
object Option3_2 {

  def lift[A, B](f: A => B) : Option[A] => Option[B] = _ map f

  def f1(a: Int, b : Int) : Double = {
    //lift(math.abs) : Option[Double] => Option[Double]
   // math.abs : Double => Double
    0.0
  }

}
