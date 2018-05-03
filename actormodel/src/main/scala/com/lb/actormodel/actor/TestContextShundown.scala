package com.lb.actormodel.actor

import akka.actor.{Actor, ActorSystem, Props}

/**
  * 说明: 
  * Created by LiuBing on 2018/2/28.
  */
class TestContextShundown extends Actor{


  override def receive: Receive = {
    case i : Int =>
      println(i)
      context.system.terminate()
      println(s"ooooo-$i")
  }

}

object TestContextShundown {

  val sys = ActorSystem("liub")
  val ref = sys.actorOf(Props[TestContextShundown])


  def main(args: Array[String]): Unit = {
    for(i <- 0 to 10){
      ref ! i
    }
Thread.sleep(10000)
    for(i <- 10 to 20){
      ref ! i
    }

  }

}
