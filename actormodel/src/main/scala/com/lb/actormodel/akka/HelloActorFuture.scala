package com.lb.actormodel.akka

import akka.actor._
import akka.util.Timeout
import akka.pattern.ask

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

/**
  * 说明: 
  * Created by LiuBing on 2018/1/9.
  */
class HelloActorFuture extends Actor with ActorLogging {

  override def receive: Receive = {
    case u: User =>
      println(s"${u.name}")
      Thread.sleep(1000 * 10);
      sender() ! u.age + 10
  }
}

object HelloActorFuture extends App {
  val as = ActorSystem("liub")

  // 通过隐式转换 传递给result
  implicit val timeout = Timeout(5 seconds)

  val asRef = as.actorOf(Props[HelloActorFuture], "123")

  // 执行一个异步actor, 会通过隐式转换调用timeout
  val future = Future {
    val a = asRef ? User("haha", 20)
    println("======")
    Thread.sleep(500)
    println(a)
    a
  }

  // 等待future在规定的时间内返回
  val result = Await.result(future, 1 second)
  // 回调函数, 在future执行结束后调用
  result onComplete {
    case Success(value) => println(value)
    case Failure(e) => e.printStackTrace
  }

  println("12312312312312312")

  as.terminate()

}