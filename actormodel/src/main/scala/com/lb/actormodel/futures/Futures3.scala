package com.lb.actormodel.futures

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, future}
import scala.util.{Failure, Success}

/**
 * Created by liubing on 16-10-8.
 */
object Futures3 {

  def main(args: Array[String]): Unit = {

    def longRunningComputation(i: Int): Future[Int] = future{
     // Thread.sleep(100)
      println(i)
      1 + i
    }

    longRunningComputation(11) onComplete {
      case Success(value) => println(s"value : $value")
      case Failure(e) => e.printStackTrace()
      case _ => println("11111111111111")
        Thread.sleep(1000)
    }
  }

}
