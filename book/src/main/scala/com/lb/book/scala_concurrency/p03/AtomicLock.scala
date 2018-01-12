package com.lb.book.scala_concurrency.p03

import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.forkjoin.ForkJoinPool

/**
  * 说明: 
  * Created by LiuBing on 2017/11/10.
  */
object AtomicLock extends App {

  //val execute  = ForkJoinPool
  private val lock = new AtomicBoolean(false)

  def mySync(body: => Unit): Unit = {
    println("111")
    while (!lock.compareAndSet(false, true)) {
      println(lock.get())
    }
    try body finally lock.set(false)
  }


  var count = 0

  //for (i <- 0 until 10) execute {
    for (i <- 0 until 10)  {
    mySync(count += i)
  }

  Thread.sleep(1000)

  println(s"count : $count")

}
