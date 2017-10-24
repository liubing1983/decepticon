package com.lb.book.scala_function.p02

/**
  * 说明: 高阶函数
  * Created by LiuBing on 2017/10/24.
  */
class Hof {

  /**
    * 计算阶乘
    *
    * @param n
    * @return
    */
  def factorial(n: Int): Int = {
    // 内部函数
    def go(n: Int, acc: Int): Int = {
      println("============" + n + "--" + acc)
      if (n <= 0) acc
      // 尾递归
      else go(n - 1, n * acc)
    }

    println("-----" + n)
    go(n, 1)
  }

  /**
    * p17 练习2.1  斐波那契数列
    *
    * @param n
    * @return
    */
  def fib(n: Int): Int = {
    def go(a: Int, b: Int, n: Int): Int = {
      println(a + "-" + b + "-" + n)
      if (n <= 0) a + b
      else go(b, a + b, n - 1)
    }

    // 斐波那契数列起始值为0和1
    go(0, 1, n)
  }


  // 2---------------------------

  private def formatAbs(x : Int)={
    val msg = "%d, %d"
    msg.format(x, Math.abs(x))
  }

  private def formatFactorial(x : Int)={
    val msg = "%d, %d"
    msg.format(x, Math.negateExact(x))
  }

  // 将上面两个函数修改为高阶函数
  private def formatF(x: Int, f: Int => Int)={
    val msg = "heheh ,  %d, %d"
    msg.format(x, f(x))

    println( msg.format(x, f(x)))
  }
}

object Hof extends App {
  val h = new Hof
  // println(h.factorial(10))

  println(h.fib(5))

  // 2-------------------------
  h.formatAbs(-1)
  h.formatFactorial(2)
  h.formatF(-1, Math.abs)

}
