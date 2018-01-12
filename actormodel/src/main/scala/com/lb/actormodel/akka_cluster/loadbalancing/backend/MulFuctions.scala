package com.lb.actormodel.akka_cluster.loadbalancing.backend

import akka.actor.{Actor, ActorLogging}
import com.lb.actormodel.akka_cluster.loadbalancing.Messages.{Add, Div, Mul, Sub}

/**
  * 说明: 
  * Created by LiuBing on 2018/1/12.
  */
class MulFuctions extends  Actor with  ActorLogging{

  override def receive: Receive = {
    case Add(x,y) =>
      println(s"$x + $y carried out by ${self} with result=${x+y}")
    case Sub(x,y) =>
      println(s"$x - $y carried out by ${self} with result=${x - y}")
    case Mul(x,y) =>
      println(s"$x * $y carried out by ${self} with result=${x * y}")
    case Div(x,y) =>
      println(s"$x / $y carried out by ${self} with result=${x / y}")
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    println(s"Restarting calculator: ${reason.getMessage}")
    super.preRestart(reason, message)
  }

}
