package com.lb.actormodel.futures

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Random

/**
 * Created by liubing on 16-10-8.
 * 并发
 * 运行一个任务，　但不阻塞
 * 使用两种回调方法
 */
object Futures2 {

  def main(args: Array[String]): Unit ={
    val f = Future{
      Thread.sleep(Random.nextInt(500))
      if(Random.nextInt(500) > 250) throw  new Exception("Error") else  1+1
    }

    /**
     * 方法１

    f.onComplete{
      case Success(value) => println(s"value : $value")
      case Failure(e) => e.printStackTrace()
    }*/

    /**
     * 方法２
     */

    f onSuccess {
      case result => println(s"value : $result")
    }

    f onFailure{
      case e => println(e.getMessage)
    }

    println("1"); Thread.sleep(100)
    println("2"); Thread.sleep(100)
    println("3"); Thread.sleep(100)
    println("4"); Thread.sleep(100)
    println("5"); Thread.sleep(100)
    println("6"); Thread.sleep(100)
    println("7"); Thread.sleep(100)
    println("8"); Thread.sleep(100)
    println("9"); Thread.sleep(100)
  }
}
