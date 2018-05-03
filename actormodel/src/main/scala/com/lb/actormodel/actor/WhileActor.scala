package com.lb.actormodel.actor

import akka.actor.{Actor, ActorSystem, Props}
import akka.routing.RoundRobinPool

/**
  * 说明: 
  * Created by LiuBing on 2018/3/23.
  */
class WhileActor extends Actor {

  override def receive: Receive = {
    case "a" => Thread.sleep(1000)
  }

}

object WhileActor{

  val sys = ActorSystem("ask-test")
  val ref = sys.actorOf(Props[WhileActor].withRouter(new RoundRobinPool(10)), name = "test")

  def main(args: Array[String]): Unit = {
    while (true){
      ref ! "a"
    }
  }
}
