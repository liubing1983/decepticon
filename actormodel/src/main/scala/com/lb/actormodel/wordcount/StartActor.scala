package com.lb.actormodel.wordcount

import scala.concurrent.duration._
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.routing. RoundRobinPool
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * 说明: 
  * Created by LiuBing on 2018/2/6.
  */
case class Msg()
case class RespMsh()
case class SelectRef()

class StartActor  extends  Actor{

  val wc = context.actorOf(Props[WordCountActor].withRouter(new RoundRobinPool(5)), "haha")
  var count = 0
  var respCount = 0

  override def receive: Receive = {
    case s: String => wc ! s
    case i: Int => println("count:"+i); count = count + i
    case SelectRef => respCount = 0; context.actorSelection("*") ! Msg
    case RespMsh =>respCount = respCount + 1 ; println("RespMsh---------------------------:"+respCount);
  }
}

object  StartActor {
  private var system : ActorSystem = _
  var ref: ActorRef = _
  def create(): Unit ={
    system = ActorSystem("wordcount")
    ref = system.actorOf(Props[StartActor], "hehe")

    //system.scheduler.schedule(0 seconds, 0.1 seconds, ref, "asdf sadfa asdf asdfa sdf f")
    for(i <- 0 to 10000){
      ref !  "asdf sadfa asdf asdfa sdf f"
    }

    system.scheduler.schedule(0 seconds, 1 seconds, ref, SelectRef)

    system.scheduler.schedule(1 seconds, 1 seconds, ref, RespMsh)
  }

  def getRef(): ActorRef={ref}


  def shutdown(): Unit ={
    system.terminate()
  }

}