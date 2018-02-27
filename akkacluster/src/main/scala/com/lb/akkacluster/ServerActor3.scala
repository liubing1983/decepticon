package com.lb.akkacluster

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.cluster.Cluster
import akka.cluster.client.ClusterClientReceptionist
import com.typesafe.config.ConfigFactory

/**
  * 说明: 
  * Created by LiuBing on 2018/2/27.
  */
class ServerActor3 extends Actor with  ActorLogging{
  val cluster = Cluster(context.system)
  override def preStart(): Unit = {
    super.preStart()
  }

  override def postStop(): Unit = {
    super.postStop()
  }

  override def receive: Receive = {
    case  job : TransformationJob =>
      val send = sender()
      Thread.sleep(1000)
      println(s"other 1 - ${job.text}")
      send ! TransformationResult("other")
  }
}

object ServerActor3 {

  def main(args: Array[String]): Unit = {

    val seednodeSetting = "akka.cluster.seed-nodes = [akka.tcp://liub@127.0.0.1:2551]"


    val config = ConfigFactory.parseString(s"akka.remote.netty.tcp.port = 0")
      //.withFallback(ConfigFactory.parseString(seednodeSetting))
      .withFallback(ConfigFactory.load("cluster.conf"))

    val clusterSystem = ActorSystem("liub", config)
    val eventListener = clusterSystem.actorOf(Props[ServerActor3], "eventListener")

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
