package com.lb.megator.function_in_scala.p04

/**
  * Created by samsung on 2017/5/16.
  */
object TestException {

  def failingFn(i : Int) : Int ={
    val y : Int = throw new Exception("fail")
    try{
      val x = 42 + 5
      x + y
    } catch {case e : Exception => 43}
  }

  def main(args: Array[String]){

    failingFn(12)
  }
}
