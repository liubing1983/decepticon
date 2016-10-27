package com.lb.actormodel.actor

import akka.actor.{Props, Actor, ActorSystem}


/**
 * Created by liubing on 16-10-8.
 * 定义一个名称为HelloActor的Actor
 */
class HelloActor() extends Actor {

  // 实现receive，　来执行ａｃｔｏｒ的动作
  def receive = {
    case "lb" => println(s"hello lb")
    case s => println(s)
  }
}

class HelloActor2(str : String = "str") extends Actor {

  // 实现receive，　来执行ａｃｔｏｒ的动作
  def receive = {
    case "lb" => println(s"hello lb $str")
    case s => println(s)
  }
}


object TestActor {

  def main(args: Array[String]) {

    //创建一个启动需要的ActorSystem, 并给他命名
    val system = ActorSystem("HelloSystem")

    // actorOf异步启动一个actor
    // actor被创建是会自动启动, 无需start或run
    val helloActor = system.actorOf(Props[HelloActor], name = "helloactor")
    // 另外一种调用方式, 构造函数包含参数
    val helloActor2 = system.actorOf(Props(new HelloActor2("1")), name = "helloactor2")

    // 通过!给actor发送信息
    helloActor ! "lb"
    helloActor ! "Hello World"

    helloActor2 ! "lb"

    // 关闭ActorSystem
    system.shutdown()

  }
}