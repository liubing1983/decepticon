package com.lb.actormodel.actor
import akka.actor._

class MessageA extends Actor{
  val sysref = context.actorOf(Props[MessageB])
  override def receive: Receive = {
    case s:String => sysref ! s
  }
}

class MessageB extends Actor{
  override def receive: Receive = {
    case s: String => println(s)
  }
}

/**
  * 说明: 
  * Created by LiuBing on 2018/1/23.
  */
object ForwardTest {

}
