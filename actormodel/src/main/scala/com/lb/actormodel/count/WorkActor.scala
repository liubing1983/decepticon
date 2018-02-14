package com.lb.actormodel.count

import java.util.concurrent.atomic.LongAdder

import akka.actor.{Actor, ActorLogging}

/**
  * 说明: 
  * Created by LiuBing on 2018/2/7.
  */
class WorkActor extends Actor  with ActorLogging{

  override def postStop(): Unit = {
    sender ! Stop
    super.postStop()
  }

  override def receive: Receive = {
    case Job =>  println(Thread.currentThread().getName  +"---11111111")
      Thread.sleep(1000)

      sender ! Stop
  }



}
