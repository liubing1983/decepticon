package com.lb.book.scala_function.p03

import com.lb.book.scala_function.p03.List._

/**
  * 说明: 
  * Created by LiuBing on 2017/10/25.
  */
object TestList extends App {

  def convertByteArray(n: Int): Array[Byte] = {
    val buf = new Array[Byte](4)
    var i = 0
    while ( {
      i < buf.length
    }) {
      buf(i) = (n >> i & 0xff).toByte

      {
        i += 1;
        i - 1
      }
    }
    buf
  }


  //   ------------------------  调用  -----------------------------
  var lb = List(1, 2, 3, 4, 5, 6)
  println(tail(lb))
  println(setHead(lb, 7))

  println(drop(lb, 3))

  println(dropWhile(lb, (x: Int) => x > 5))


}
