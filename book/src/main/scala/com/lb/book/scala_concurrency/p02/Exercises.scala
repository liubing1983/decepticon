package com.lb.book.scala_concurrency.p02


import java.util.concurrent.{Callable, ExecutorService, Executors, FutureTask}

import scala.concurrent._


class Exercises[A, B] {


//  def parallel[A, B](a: => A, b: => B): (A, B) = {
//
//    val pool: ExecutorService = Executors.newFixedThreadPool(2)
//    try {
//      //创建两个有返回值的任务
//      val future = new FutureTask[A](new Callable[A] {
//        override def call(): A = a
//      })
//      //pool.execute(future)
//      //println(future.get())
//    } finally {
//      pool.shutdown()
//    }
    //Callable c2 = new MyCallable("B");

   // (11,22)
//  }
}

/**
  * 说明: 
  * Created by LiuBing on 2017/11/14.
  */
object Exercises extends App {
  val e = new Exercises[Int, String]
 // println(e.parallel[Int, String](a, b).toString())

  def a(): Int = {
    1 + 1
  }

  def b(): String = {
    "Hello" + ", world"
  }

}
