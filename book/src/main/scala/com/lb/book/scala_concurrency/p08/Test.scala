package com.lb.book.scala_concurrency.p08

import akka.actor.{ActorSystem, Props}

/**
  * 说明: 
  * Created by LiuBing on 2018/1/4.
  */
object Test   extends App{

    val as = ActorSystem("liub")


    val asRef = as.actorOf(Props[HelloActor], "123")

    asRef ! "hehe"
    asRef ! "hehe2"
    asRef ! 123
}
