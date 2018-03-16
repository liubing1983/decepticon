package com.lb.akkacluster

import java.util.concurrent.atomic.LongAdder

import akka.actor
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.cluster.client.ClusterClientReceptionist
import akka.routing.RoundRobinPool
import com.typesafe.config.ConfigFactory

/**
  * 说明: 
  * Created by LiuBing on 2018/2/27.
  */
class ServerActor extends Actor with  ActorLogging{
  val cluster = Cluster(context.system)
  var count = new LongAdder
  override def preStart(): Unit = {
    cluster.subscribe(self,initialStateMode = InitialStateAsEvents
      ,classOf[MemberEvent],classOf[UnreachableMember])  //订阅集群状态转换信息
    super.preStart()
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)    //取消订阅
    println("1111111111111111111111111")
    sender ! TransformationResult("other")
    super.postStop()
  }

  override def receive: Receive = {

    case  job : TransformationJob =>
      val send = job.ref
      count.add(1)
      Thread.sleep(100)
      println(s"2551 - ${job.text} - ${count.sum()}")
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

    val seednodeSetting = "akka.cluster.seed-nodes = [\"akka.tcp://liub@127.0.0.1:2551\",\"akka.tcp://liub@127.0.0.1:2552\"]"

    val config = ConfigFactory.parseString(s"akka.remote.netty.tcp.port = 2551")
      .withFallback(ConfigFactory.parseString(seednodeSetting))
      .withFallback(ConfigFactory.load("cluster.conf"))

    val clusterSystem = ActorSystem("liub", config)
    val eventListener = clusterSystem.actorOf(Props[ServerActor].withRouter(new RoundRobinPool(10) )
      .withDispatcher("liub-dispatcher"), "eventListener")

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