package com.lb.actormodel.actor

import akka.actor._

class LbChild extends Actor {

  override def preStart {
    println("start  child  actor ")
  }

  override def postStop {
    println(s" ${self.path}")
  }

  def receive = {
    case Terminated(self) => println("kill self")
    case _ => println(s" =====")
  }

}

class LbParent extends Actor {
  val child = context.actorOf(Props[Child], name = "child")
  context.watch(child)

  def receive = {
    case Terminated(child) => println(s"kill  child")
    case CreateChild(name) => println(name)
    case _ => println("other")
  }
}

/**
 * Created by liubing on 16-10-9.
 */
object WatchTest {
  def main(args: Array[String]): Unit = {
    val as = ActorSystem("start-test")
    val p = as.actorOf(Props[LbParent], name = "parent")

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
