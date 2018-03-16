package com.lb.akkacluster

import java.util.concurrent.atomic.LongAdder

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.cluster.Cluster
import akka.cluster.client.ClusterClientReceptionist
import akka.routing.RoundRobinPool
import com.typesafe.config.ConfigFactory

/**
  * 说明: 
  * Created by LiuBing on 2018/2/27.
  */
class ServerActor3 extends Actor with  ActorLogging{
  val cluster = Cluster(context.system)
  var count = new LongAdder
  override def preStart(): Unit = {
    super.preStart()
  }

  override def postStop(): Unit = {
    println("33333333333333333333333")
    sender ! TransformationResult("other")
    super.postStop()
  }

  override def receive: Receive = {
    case  job : TransformationJob =>
      val send = sender()
      count.add(1)
      Thread.sleep(100)
      println(s"other 1 - ${job.text} - ${count.sum()}")
      send ! TransformationResult("other")
  }
}

object ServerActor3 {

  def main(args: Array[String]): Unit = {

    val seednodeSetting = "akka.cluster.seed-nodes = [\"akka.tcp://liub@127.0.0.1:2551\"" +
                                                   " ,\"akka.tcp://liub@127.0.0.1:2552\"]"


    val config = ConfigFactory.parseString(s"akka.remote.netty.tcp.port = 0")
      .withFallback(ConfigFactory.parseString(seednodeSetting))
      .withFallback(ConfigFactory.load("cluster.conf"))

    val clusterSystem = ActorSystem("liub", config)
    val eventListener = clusterSystem.actorOf(Props[ServerActor3]
      .withRouter(new RoundRobinPool(10) ).withDispatcher("liub-dispatcher"), "eventListener")

  //  val cluster = Cluster(clusterSystem)
   // cluster.registerOnMemberRemoved(println("Leaving cluster. I should cleanup... "))
   // cluster.registerOnMemberUp(println("Hookup to cluster. Do some setups ..."))


    // 注册集群客户端
    ClusterClientReceptionist.get(clusterSystem).registerService(eventListener)

    println("actor system started!")
 //   scala.io.StdIn.readLine()

 //   clusterSystem.terminate()
  }


}
