package com.lb.actormodel.akka_cluster.demo.server

import akka.actor.{Actor, ActorLogging, ActorSystem, Props, RootActorPath}

import language.postfixOps
import scala.concurrent.duration._
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberEvent, MemberUp}
import akka.cluster.Member
import akka.cluster.MemberStatus
import com.lb.actormodel.akka_cluster.demo.{BackendRegistration, TransformationJob, TransformationResult}
import com.lb.actormodel.transformation.TransformationBackend
import com.typesafe.config.ConfigFactory
import akka.event.Logging
import akka.event.LoggingAdapter

/**
  * 说明: 
  * Created by LiuBing on 2018/1/11.
  */
class ClusterServer extends Actor with ActorLogging{


 // val log: LoggingAdapter = Logging.getLogger(context.system, this)
  val cluster = Cluster(context.system)

  override def preStart(): Unit = cluster.subscribe(self, classOf[MemberEvent])  //在启动Actor时将该节点订阅到集群中
  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive = {
    case TransformationJob(text) => {
      val result = text.toUpperCase // 任务执行得到结果（将字符串转换为大写）
      log.info(s"${result}----------------------")
      sender() ! TransformationResult(text.toUpperCase) // 向发送者返回结果
    }
    case state: CurrentClusterState =>
      state.members.filter(_.status == MemberStatus.Up) foreach register // 根据节点状态向集群客户端注册
      log.info(s"---------------sdfsdfsfsfsfsfsfsf-------")
    case MemberUp(m) => register(m)
    log.info(m+"---------------------------")// 将刚处于Up状态的节点向集群客户端注册
  }

  def register(member: Member): Unit = {   //将节点注册到集群客户端
    context.actorSelection(RootActorPath(member.address) / "user" / "frontend") !
      BackendRegistration
  }
}

object ClusterServer extends App {
  val config = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=0").
    withFallback(ConfigFactory.load("cluster.conf"))

  val system = ActorSystem("liub", config)
  system.actorOf(Props[ClusterServer], name = "backend")
}
