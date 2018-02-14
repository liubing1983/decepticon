package com.lb.actormodel.wordcount

import akka.actor.Actor

/**
  * 说明: 
  * Created by LiuBing on 2018/2/6.
  */
class WordCountActor  extends  Actor{

  override def receive: Receive = {
    case s:String =>
      println("s: " +s)
       sender ! s.split(" ", -1).length
    case Msg => println("msg");sender() ! RespMsh
  }
}
