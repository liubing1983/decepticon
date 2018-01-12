package com.lb.book.scala_function.p03


/**
  * 说明: 
  * Created by LiuBing on 2017/10/25.
  */
// 抽象接口, 定义了一个没有实现任何方法的List特质
sealed trait List[+A]

// 定义了两种构造器

case object Nil extends List[Nothing]

case class Cons[+A](head: A, tail: List[A]) extends List[A]

object List {
  def sum(ints: List[Int]): Int = ints match {
    case Nil => 0
    case Cons(x, xs) => x + sum(xs)
  }

  def product(ds: List[Double]): Double = ds match {
    case Nil => 0.0
    case Cons(0.0, _) => 0.0
    case Cons(x, xs) => x * product(xs)
  }

  def apply[A](as: A*): List[A] = if (as.isEmpty) Nil else Cons(as.head, apply(as.tail: _*))

  /**
    * p30  练习3.2 删除list的第一个元素
    *
    * @param l
    * @tparam A
    * @return
    */
  def tail[A](l: List[A]): List[A] = {
    l match {
      case Nil => Nil
      // 下面两个选项, 结果一样
      case Cons(_, b) => b
      //case Cons(a, b) => if(b == Nil) x else  b
    }
  }

  /**
    * p30  练习3.3
    *
    * @param l
    * @param x
    * @tparam A
    * @return
    */
  def setHead[A](l: List[A], x: A): List[A] = {
    l match {
      case Nil => Nil
      case Cons(a, b) => Cons(x, b)
    }
  }

  /**
    * p30 练习3.5
    *
    * @param l
    * @param n
    * @tparam A
    * @return
    */
  def drop[A](l: List[A], n: Int): List[A] = {
    def loop[A](x: List[A], m: Int): List[A] = {
      if (m >= n) x
      else {
        x match {
          case Nil => Nil
          case Cons(_, t) => loop(t, m + 1)
        }
      }
    }

    loop(l, 0)
  }

  def dropWhile[A](l: List[A], f: A => Boolean): List[A] = {
    l match {
      case Cons(_, Nil) => l
      case Cons(a, b) => if(f(a)) dropWhile(b, f) else  dropWhile(l, f)
    }
  }

}
