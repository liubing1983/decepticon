package com.lb.megator.function_in_scala.p03

/**
  * Created by samsung on 2017/4/27.
  */
object TestList {
  def main(args: Array[String]): Unit = {
    List(1, 2, 3) match {
      case Cons(_, t) => println(t); t
    }


    val x = List(1, 2, 3, 4, 5) match {
      case Cons(x, Cons(2, Cons(4, _))) => println("1"); x
      case Nil => println("2"); 42
      case Cons(x, Cons(y, Cons(3, Cons(4, _)))) => println("3"); x + y
      case Cons(h, t) => println("4"); h + List.sum(t)
      case _ => println("5"); 101
    }

    println("2: " + x)
  }
}
