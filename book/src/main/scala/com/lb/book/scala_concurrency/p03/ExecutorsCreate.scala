package com.lb.book.scala_concurrency.p03

import java.util.concurrent.TimeUnit

import scala.concurrent.forkjoin.ForkJoinPool

/**
  * 说明: 
  * Created by LiuBing on 2017/11/10.
  */
object ExecutorsCreate extends App {

  /**
    * ForkJoinPool 继承了 ExecutorService
    * ForkJoinPool 在程序结束时默认调用shutdown,  无需手动调用
    */
  val executor = new ForkJoinPool(2)

  executor.execute(new Runnable {
    override def run(): Unit = {
      println("forkjoin  start")
      Thread.sleep(3000)
      println("forkjoin  end")
    }
  })

  println("1231231231")
  executor.shutdown()
  executor.awaitTermination(1, TimeUnit.SECONDS)
  println("22223333")




}
