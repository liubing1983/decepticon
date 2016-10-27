package com.lb.actormodel.futures

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Created by liubing on 16-10-8.
 * 并发
 * 运行一个任务，　但阻塞
 */
object Futures1 {

  def main(args : Array[String]): Unit ={
    implicit val basetime = System.currentTimeMillis()

    val f = Future{
      Thread.sleep(3500)
      1+1
    }

    // Await.result当ｆ　１秒之内没有返回数据，　则抛错
    val result = Await.result(f, 1 second)

    println(result)

    Thread.sleep(1000)
  }

}
