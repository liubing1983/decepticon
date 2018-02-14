package com.lb.actormodel.count

import java.util.concurrent.atomic.LongAdder

import akka.actor.Actor.Receive
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.routing.RoundRobinPool

/**
  * 说明: 测试actor计数器,  所有actor执行完成后
  * Created by LiuBing on 2018/2/7.
  */

case class Stop()

case class Job()

case class Msg(s: String)

class StartActor extends Actor with ActorLogging {

  var count = new LongAdder
  val workRef = context.actorOf(Props[WorkActor].withRouter(new RoundRobinPool(3)), "test")

  override def postStop(): Unit = {
    super.postStop()
  }

  override def receive: Receive = {
    case Job => count.add(1) // 发送一个任务计数器加一
      workRef ! Job
    case Stop =>println(count.sum().toString+"-------------");
      println("-----============================--------");
      count.decrement() // 任务完成后计数器减一
    case "count" =>
      println(count.sum().toString)
      if (count.sum() <= 0) { // 判断任务是否全部完成
        //StartActor.b = false
        //StartActor.lb()
      }
  }
}

object StartActor {
  val sys = ActorSystem.create("lb")
  val ref = sys.actorOf(Props[StartActor].withRouter(new RoundRobinPool(3)))
  var b = true

  def main(args: Array[String]): Unit = {
    // 发送任务
    for (i <- 0 until 50) {
      println(i+"---***---***---")
      ref ! Job
    }

    // 循环监控任务列表
    while (b) {
      ref ! "count"
      Thread.sleep(500)
    }

  }

  // 停止actor集群
  def lb(): Unit = {
    println("789456123321654987/*-")
    sys.terminate()
  }
}
