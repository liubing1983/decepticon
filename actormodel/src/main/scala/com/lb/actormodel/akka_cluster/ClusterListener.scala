package com.lb.actormodel.akka_cluster

import akka.actor.{Actor, ActorLogging}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._

/**
  * 说明: 
  * Created by LiuBing on 2018/1/3.
  */
class ClusterListener extends Actor with  ActorLogging{

  val cluster = Cluster(context.system)

  override def preStart(): Unit = {
    cluster.subscribe(self, classOf[MemberEvent], classOf[UnreachableMember])
    super.preStart()
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
    super.postStop()
  }

  override def receive: Receive = {
    case s : String => log.info(s"String: type,  value: ${s}")
    case state: CurrentClusterState => log.info("Current members: {}", state.members.mkString(", "))
    case MemberUp(member) => log.info("Member is Up: {}", member.address)
    case UnreachableMember(member) => log.info("Member detected as unreachable: {}", member)
    case MemberRemoved(member, previousStatus) =>  log.info("Member is Removed: {} after {}", member.address, previousStatus)
    case _: MemberEvent => // ignore
  }
}
