package com.lb.megator.test

/**
  * Created by samsung on 2017/6/14.
  */
object TestTuple {

  def main(args: Array[String]): Unit ={
    val a = "123456789";

   println( a.split("").map(_.toInt).reduce(_+_))


    val b: Array[String] = a.split("")
    var c =0
    //for(i:Int = 0; i<= b.length; i++){
      //c = b[i] + c
    //}

  }

}
