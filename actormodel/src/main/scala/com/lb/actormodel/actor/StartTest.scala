package com.lb.actormodel.actor

import akka.actor.{PoisonPill, ActorSystem, Props, Actor}

case class CreateChild(name: String)

case class Name(name: String)

class Child extends Actor {
  var name = "No name"

  override def postStop {
    println(s"$name  --- ${self.path}")
  }

  def receive={
    case Name(name) => this.name = name
    case _ => println(s" $name   =====")
  }

}

class Parent extends Actor {
  def receive = {
    case CreateChild(name) => {
      println(s"create child $name")
      val child = context.actorOf(Props[Child], name = "child")
      child ! Name(name)
    }
    case _ => println("other")
  }
}



/**
 * Created by liubing on 16-10-9.
 */
object StartTest {

def main(args: Array[String]): Unit ={
  val as = ActorSystem("start-test")
  val p = as.actorOf(Props[Parent], name = "parent")

  p ! CreateChild("liu bing")
  //p ! CreateChild("milan")
  Thread.sleep(1000)

  println("sending")

  // 找到路径下的actor
  val j = as.actorSelection("/user/parent/child")
  j ! PoisonPill
  println("j was killed")

  Thread.sleep(5000)

  as.shutdown()

}
}


