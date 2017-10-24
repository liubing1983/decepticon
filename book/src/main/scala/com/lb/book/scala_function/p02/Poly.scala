package com.lb.book.scala_function.p02

/**
  * 说明: 多态函数
  * Created by LiuBing on 2017/10/24.
  */
class Poly {

  /**
    * 在数组中查找字符串
    *
    * @param ss
    * @param key
    * @return
    */
  def findFirst(ss: Array[String], key: String): Int = {
    @annotation.tailrec
    def loop(n: Int): Int = {
      if (n >= ss.length) -1
      else if (ss(n) == key) n
      else loop(n + 1)
    }

    loop(0)
  }

  /**
    * 使用泛型函数[多态函数]来实现 数组中查找字符串
    *
    * @param ss
    * @param key
    * @tparam A
    * @return
    */
  def findFirstPoly[A](ss: Array[A], key: A): Int = {
    @annotation.tailrec
    def loop(n: Int): Int = {
      if (n >= ss.length) -1
      else if (ss(n) == key) n
      else loop(n + 1)
    }

    loop(0)
  }

  def findFirstPoly2Hof[A](ss: Array[A], f: A => Boolean): Int = {
    @annotation.tailrec
    def loop(n: Int): Int = {
      if (n >= ss.length) -1
      else if (f(ss(n))) n
      else loop(n + 1)
    }
    loop(0)
  }

  /**
    * p20 练习2.2  判断数组是否按照提供的比较函数排序
    * @param as
    * @param ordered
    * @tparam A
    * @return
    */
  def isSorted[A](as: Array[A], ordered:(A, A)=> Boolean): Boolean={
    def loop(m: Int, n: Int): Boolean ={
      if(m >= as.length-1) true
      else if(!ordered(as(m), as(n)))   false
      else loop(n, n+1)
    }
    loop(0, 1)
  }
}

object Poly extends App {
  val p = new Poly
  println(p.findFirstPoly[Int](Array(1, 2, 3, 4, 5), 47))

  def f1[Int](i: Int): Boolean = {
    i == 4
  }

  println(p.findFirst(Array("1", "2", "3", "4", "5"), "2"))
  println(p.findFirstPoly[Int](Array(1, 2, 3, 4, 5), 2))
  // 使用匿名函数传递
  println(p.findFirstPoly2Hof[Int](Array(1, 2, 3, 4, 5), (i: Int) => i == 2))
  // 使用函数传递
  println(p.findFirstPoly2Hof[Int](Array(1, 2, 3, 4, 5), f1))

  println(p.isSorted[Int](Array(1, 5, 3, 7), (m: Int, n: Int) => m< n))
}