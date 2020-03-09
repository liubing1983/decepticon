package com.lb.akkacluster


import java.util.concurrent.atomic.{AtomicInteger, LongAdder}

import akka.actor.{Actor, ActorPath, ActorSystem, Props}
import akka.cluster.Cluster
import akka.cluster.client.{ClusterClient, ClusterClientSettings}
import akka.routing.RoundRobinPool
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

/**
  * 说明: 
  * Created by LiuBing on 2018/2/27.
  */
class ClientActor extends Actor {

  val initialContacts = Set(ActorPath.fromString("akka.tcp://liub@127.0.0.1:2551/system/receptionist"),
                            ActorPath.fromString("akka.tcp://liub@127.0.0.1:2552/system/receptionist"))
  val settings = ClusterClientSettings(context.system).withInitialContacts(initialContacts)
    // ref过期时间, 默认30s
    .withReconnectTimeout(Option(new FiniteDuration(6000000L, MILLISECONDS )))
    // 集群心跳时间, 可接受的心跳暂停, 默认2是, 13s
  .withHeartbeat(new FiniteDuration(10L, SECONDS ), new FiniteDuration(30L, SECONDS ))
  .withBufferSize(10000)

 // ConfigFactory.parseString("""akka.cluster.use-dispatcher = liub2-dispatcher""")


  println(settings.bufferSize)
  println(settings.heartbeatInterval._1 + "-"+ settings.heartbeatInterval._2 + "-"+ settings.heartbeatInterval.length)
  println(settings.reconnectTimeout.mkString("-"))
  println(settings)

  val c = context.system.actorOf(ClusterClient.props(settings)
    .withRouter(new RoundRobinPool(2)).withDispatcher("liub2-dispatcher")
    ,"client")

  println(context.dispatcher.toString+"===---==")

  var count = new LongAdder
  var sum = new LongAdder

  override def receive: Receive = {
    case s: String =>
      println("-----------------")
      for(i <- 0 to 90000){
        c ! ClusterClient.Send("/user/eventListener", TransformationJob(s+i.toString, self), false)
        count.add(1)
      //  sum.add(1)
       // if(count.sum() >= 1000) Thread.sleep(10000)
      }

    case job: TransformationResult =>
      count.decrement()
      println(count.sum()+"----")

      if(count.sum() <= 0){
        ClientActor.stop()
      }
  }
}

object ClientActor {
  val system = ActorSystem("qwe", ConfigFactory.load("client.conf"))
  val ref = system.actorOf(Props[ClientActor], "client-object")


  val counter = new AtomicInteger
  val s1 = System.currentTimeMillis()


  def main(args: Array[String]): Unit = {

    //for(i <- 0 to 10000 ){
      ref ! s"hello"
    //}
  }

  def stop(): Unit ={
    println(System.currentTimeMillis() - s1)
    val cluster = Cluster(system)
    cluster.leave(cluster.selfAddress)
    system.terminate()
  }
}

