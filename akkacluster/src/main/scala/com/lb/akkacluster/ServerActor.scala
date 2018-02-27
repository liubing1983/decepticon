package com.lb.akkacluster

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.cluster.client.ClusterClientReceptionist
import com.typesafe.config.ConfigFactory

/**
  * 说明: 
  * Created by LiuBing on 2018/2/27.
  */
class ServerActor extends Actor with  ActorLogging{
  val cluster = Cluster(context.system)
  override def preStart(): Unit = {
    cluster.subscribe(self,initialStateMode = InitialStateAsEvents
      ,classOf[MemberEvent],classOf[UnreachableMember])  //订阅集群状态转换信息
    super.preStart()
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)    //取消订阅
    super.postStop()
  }

  override def receive: Receive = {

    case  job : TransformationJob =>
      val send = sender()
      Thread.sleep(1000)
      println(s"2551 - ${job.text}")
      send ! TransformationResult("2551")

    case MemberJoined(member) => log.info("Member is Joining: {}", member.address)
    case MemberUp(member) =>  log.info("Member is Up: {}", member.address)
    case MemberLeft(member) => log.info("Member is Leaving: {}", member.address)
    case MemberExited(member) => log.info("Member is Exiting: {}", member.address)
    case MemberRemoved(member, previousStatus) =>  log.info("Member is Removed: {} after {}",  member.address, previousStatus)
    case UnreachableMember(member) => log.info("Member detected as unreachable: {}", member)
      cluster.down(member.address)      //手工驱除，不用auto-down
    case _: MemberEvent => // ignore
  }
}

object ServerActor {

  def main(args: Array[String]): Unit = {

    val seednodeSetting = "akka.cluster.seed-nodes = [akka.tcp://liub@127.0.0.1:2551]"


    val config = ConfigFactory.parseString(s"akka.remote.netty.tcp.port = 2551")
      //.withFallback(ConfigFactory.parseString(seednodeSetting))
      .withFallback(ConfigFactory.load("cluster.conf"))

    val clusterSystem = ActorSystem("liub", config)
    val eventListener = clusterSystem.actorOf(Props[ServerActor], "eventListener")

    //val cluster = Cluster(clusterSystem)
    //cluster.registerOnMemberRemoved(println("Leaving cluster. I should cleanup... "))
    //cluster.registerOnMemberUp(println("Hookup to cluster. Do some setups ..."))


    // 注册集群客户端
    ClusterClientReceptionist(clusterSystem).registerService(eventListener)



    println("actor system started!")
   // scala.io.StdIn.readLine()

   // clusterSystem.terminate()
  }

}