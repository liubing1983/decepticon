package com.lb.akkacluster


import java.util.concurrent.atomic.{AtomicInteger, LongAdder}

import akka.actor.{Actor, ActorPath, ActorSystem, Props}
import akka.cluster.client.{ClusterClient, ClusterClientSettings}
import akka.routing.RoundRobinPool
import com.typesafe.config.ConfigFactory

/**
  * 说明: 
  * Created by LiuBing on 2018/2/27.
  */
class ClientActor extends Actor {


  val initialContacts = Set(ActorPath.fromString("akka.tcp://liub@127.0.0.1:2551/system/receptionist"),
                            ActorPath.fromString("akka.tcp://liub@127.0.0.1:2552/system/receptionist"))
  val settings = ClusterClientSettings(context.system).withInitialContacts(initialContacts)

  val c = context.system.actorOf(ClusterClient.props(settings).withRouter(new RoundRobinPool(10) ), "demo-client")

  var count = new LongAdder

  override def receive: Receive = {
    case s: String =>println("-----------------");
      c ! ClusterClient.Send("/user/eventListener", TransformationJob(s), false)
      count.add(1)
    case job: TransformationResult =>
      count.decrement()
      println(count.sum()+"----")
  }
}

object ClientActor {
  val system = ActorSystem("qwe", ConfigFactory.load("client.conf"))
  val ref = system.actorOf(Props[ClientActor], "client")


  val counter = new AtomicInteger
  val s1 = System.currentTimeMillis()

  def main(args: Array[String]): Unit = {
    for(i <- 0 to 100 ){
      ref ! s"hello-${i}"
    }


  }
}

