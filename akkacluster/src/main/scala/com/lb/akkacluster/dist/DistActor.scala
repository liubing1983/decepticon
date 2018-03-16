package com.lb.akkacluster.dist

import akka.actor.{Actor, ActorPath, ActorSystem, Props, RootActorPath}
import akka.cluster.Member
import akka.cluster.client.{ClusterClient, ClusterClientSettings}
import com.lb.akkacluster.{BackendRegistration, End, TransformationJob, TransformationResult}
import com.typesafe.config.ConfigFactory

import scala.actors.threadpool.AtomicInteger
import scala.io.StdIn
import scala.concurrent.duration._

/**
  * 说明: 
  * Created by LiuBing on 2018/2/12.
  */
class DistActor  extends Actor {

  val initialContacts = Set(
    ActorPath.fromString("akka.tcp://liub@127.0.0.1:2551/system/server"),
    ActorPath.fromString("akka.tcp://liub@127.0.0.1:2551/system/server"))
  val settings = ClusterClientSettings(context.system)
    .withInitialContacts(initialContacts)


  val c = context.system.actorOf(ClusterClient.props(settings), "demo-client")


  def receive = {
    case TransformationResult(result) => {
      println(s"Client response and the result is ${result}")
    }
    case i : Int => {
      println(s"=====================:$i")
      c ! ClusterClient.Send("/user/server", TransformationJob(i.toString , self), localAffinity = true)
    }
  }
}

object  DistActor {
  def main(args: Array[String]): Unit = {



    val system = ActorSystem("liub" , ConfigFactory.load().getConfig("Job"))
    val ref =  system.actorOf(Props[DistActor], name = "hehe")



    val counter = new AtomicInteger
    import system.dispatcher
    system.scheduler.schedule(2.seconds, 2.seconds) {   //定时发送任务
      ref ! counter.incrementAndGet()
    }
  }
}
