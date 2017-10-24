package com.lb.megator.function_in_scala.p03

/**
  * Created by samsung on 2017/4/27.
  */

/**
  * List是一个泛型的数据类型, 类型参数用A表示
  * trait:  定义一个没有方法的List特质
  * sealed: 这个特质所有的实现都必须定义在这个文件里
  * @tparam A
  */
sealed trait List[+A]

/**
  * 用于表现空List的List构造器
  */
case object Nil extends List[Nothing]

/**
  * 呈现非空List的构造器
  * @param head
  * @param tail
  * @tparam A
  */
case class Cons[+A](head: A, tail: List[A]) extends List[A]

/**
  * List的伴生对象.
  */
object List {

  /**
    * 利用模式匹配对一个整数型的List求和
    * @param ints
    * @return
    */
  def sum(ints: List[Int]): Int = {
    ints match {
      case Nil => 0
      case Cons(x, xs) => x + sum(xs)   // 尾递归
    }
  }

  def product(ds: List[Double]): Double = {
    ds match {
      case Nil => 1.0
      case Cons(0.0, _) => 0.0
      case Cons(x, xs) => x * product(xs)
    }
  }

  def apply[A](as: A*): List[A] = {
    if (as.isEmpty) Nil
    else Cons(as.head, apply(as.tail: _*))
  }



}
