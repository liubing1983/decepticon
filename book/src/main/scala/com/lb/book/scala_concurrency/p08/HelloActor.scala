package com.lb.book.scala_concurrency.p08

import akka.actor.{Actor, ActorLogging, Props}

/**
  * 说明: 
  * Created by LiuBing on 2018/1/5.
  */
class HelloActor extends Actor with  ActorLogging{

  val stringRef = context.actorOf(Props[HelloActorString], "String")
  val intRef = context.actorOf(Props[HelloActorInt], "int")

  override def receive: Receive = {
    case s:String =>  stringRef ! s
    case i:Int =>  intRef ! i
  }

}

class HelloActorString extends Actor{
  override def receive: Receive = {
    case s:String => println(s"String_${s}")
      Thread.sleep(1000 * 60)
      println(s"String_${s}_end")
  }
}

class HelloActorInt extends Actor{
  override def receive: Receive = {
    case i:Int => println(s"Int_${i}")
      Thread.sleep(1000 * 60)
      println(s"Int_${i}_end")
  }
}

