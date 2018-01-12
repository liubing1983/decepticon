package com.lb.book.scala_function.p05

/**
  * 说明: 
  * Created by LiuBing on 2017/10/26.
  */
object Test extends App {

  /**
    * if是惰性求值的
    * @param b
    * @param f1
    * @param f2
    * @tparam A
    * @return
    */
  def if2[A](b : Boolean, f1: => A, f2 : => A) : A ={
    if(b) f1 else f2
  }

  def if3[A](b : Boolean, i: Int) : Int ={
    val j = i
    if(b) j+j else 0
  }

  if2(1>1, println("a") , println("b"))
  val x = if3(true, {println("h1"); 2+3})
  println(x)


}
