package com.lb.actormodel.akka

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}



/**
  * 说明: 
  * Created by LiuBing on 2018/1/9.
  */
class HelloActor extends Actor with  ActorLogging{

  override def receive: Receive = {
    case u:User if(u.age >= 18) => log.info(s"${u.name}${u.age}岁, 是一个成人")
    case u:User=> log.info(s"${u.name}${u.age}岁, 是一个儿童")
  }

}

object HelloActor extends App{
  implicit  val as = ActorSystem("helloActor")

  val asRef = as.actorOf(Props[HelloActor], "test")

  asRef ! User("hehe", 20)
  asRef ! User("haha", 15)
  asRef ! "123"

  as.terminate()

}
