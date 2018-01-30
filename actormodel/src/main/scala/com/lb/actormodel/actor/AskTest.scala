package com.lb.actormodel.actor

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps


class TestActor extends Actor {
  def receive = {
    case "lb" => sender ! "return123"
    case _ => sender ! "other"
  }
}

/**
  * Created by liubing on 16-10-9.
  */
object AskTest {

  def main(args: Array[String]) {

    val as = ActorSystem("ask-test")
    val aaa = as.actorOf(Props[TestActor], name = "test")

    implicit val timeout = Timeout(5 seconds)
    val s = aaa ? "lb"
    println(s + "111")
    println(Await.result(s, timeout.duration).asInstanceOf[String] + "222")
    Thread.sleep(2000)


    val s2 = ask(aaa, "lb").mapTo[String]
    println(s2 + "333")
    Thread.sleep(2000)
    //as.shutdown()

  }
}
