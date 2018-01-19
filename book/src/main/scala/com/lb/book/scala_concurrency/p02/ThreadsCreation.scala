package com.lb.book.scala_concurrency.p02

/**
  * 说明: 
  * Created by LiuBing on 2018/1/18.
  */
object ThreadsCreation  extends App{

  class MyThread extends  Thread{
    override def run(): Unit = {
      Thread.sleep(500)
      println("thread")
    }
  }

  val t = new MyThread
  t.start()
  println("mail333")
  t.join()

  println("mail")
}
