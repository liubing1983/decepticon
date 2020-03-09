package com.lb.akkacluster.parse

import akka.actor.{Actor, ActorSystem, Props, RootActorPath}
import akka.cluster.{Cluster, Member, MemberStatus}
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberEvent, MemberUp}
import com.lb.akkacluster.{BackendRegistration, TransformationJob, TransformationResult}
import com.typesafe.config.ConfigFactory



/**
  * 说明: 
  * Created by LiuBing on 2018/2/12.
  */
class ClientActor extends Actor {

  val cluster = Cluster(context.system)

  override def preStart(): Unit = cluster.subscribe(self, classOf[MemberEvent])  //在启动Actor时将该节点订阅到集群中
  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive = {
    case TransformationJob(text, ref) => { // 接收任务请求
      println(s"client-${text}")
      val result = text.toUpperCase // 任务执行得到结果（将字符串转换为大写）
      sender() ! TransformationResult(text.toUpperCase) // 向发送者返回结果
    }
    case state: CurrentClusterState =>
      state.members.filter(_.status == MemberStatus.Up) foreach register // 根据节点状态向集群客户端注册
    case MemberUp(m) => register(m)  // 将刚处于Up状态的节点向集群客户端注册
  }

  def register(member: Member): Unit = {   //将节点注册到集群客户端
    context.actorSelection(RootActorPath(member.address) / "user" / "server") ! BackendRegistration
  }
}

object ClientActor{
  def main(args: Array[String]): Unit = {
    // Override the configuration of the port when specified as program argument
    //val port = if (args.isEmpty) "2553" else args(0)
    val config = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=2552").
      withFallback(ConfigFactory.load().getConfig("Client"))

    val system = ActorSystem("liub", config)
    system.actorOf(Props[ClientActor], name = "client")
  }
}
