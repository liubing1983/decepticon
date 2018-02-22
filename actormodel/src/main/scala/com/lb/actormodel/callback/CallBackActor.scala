package com.lb.actormodel.callback

import java.util.concurrent.atomic.LongAdder

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import util.control.Breaks._
case class msg(i:Int, callb:String=> Unit)

/**
  * 说明: 
  * Created by LiuBing on 2018/2/22.
  */
class CallBackActor extends Actor with ActorLogging {

  var c = new LongAdder

  override def receive = {
    case job: msg => {
      // println(s"""${println(Thread.currentThread().getName)}****${job.i}****""")
      c.add(1)
      println(c.sum()+"----")
      if (c.sum() >= 9) {
        job.callb(Thread.currentThread().getName)
        context.stop(self)
      }
    }
  }

}

object CallBackStart {

  var ref: ActorRef = _
  var b = false



  def start(callb:String => Unit) = {
    val system = ActorSystem("callbackdemo")
    var count = 0
    val ref = system.actorOf(Props[CallBackActor])
   // print(Thread.currentThread().getName+"  ")
    system.scheduler.schedule(0 seconds, 1 seconds, ref, msg(count, callb))

  }
}
