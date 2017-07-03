package com.lb.megator.function_in_scala

/**
  * Created by samsung on 2017/4/27.
  */
object TestList {
  def main(args: Array[String]): Unit ={
    List(1,2,3) match {case Cons(_, t) => println(t); t}
  }
}
