package com.lb.akkacluster.dist

import language.postfixOps
import akka.actor.ActorSystem
import akka.actor.Props
import com.typesafe.config.ConfigFactory
import akka.cluster.client.ClusterClientReceptionist

/**
  * 说明: 
  * Created by LiuBing on 2018/2/13.
  */
object TransformationFrontendApp {
  def main(args: Array[String]): Unit = {

    val port = if (args.isEmpty) "0" else args(0)
    val config = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=2551").
      withFallback(ConfigFactory.load().getConfig("Server"))

    val system = ActorSystem("liub", config)
    val frontend = system.actorOf(Props[ServerActor], name = "server")
    ClusterClientReceptionist(system).registerService(frontend)
  }
}
