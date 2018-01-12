package com.lb.actormodel.akka_cluster

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory

/**
  * 说明: 
  * Created by LiuBing on 2018/1/3.
  */
object ClusterListenerImpl extends  App{


  // 将当前节点加入actor集群
  val config = ConfigFactory.parseString(
    s"""akka.remote.netty.tcp.port=2551
        akka.remote.artery.canonical.port=2551
        """)
    .withFallback(ConfigFactory.parseString("akka.cluster.roles = [frontend]"))
    .withFallback(ConfigFactory.load("cluster.conf"))

  val as = ActorSystem("liub", config = config)
  val asRef = as.actorOf(Props[ClusterListener], "321")

}
