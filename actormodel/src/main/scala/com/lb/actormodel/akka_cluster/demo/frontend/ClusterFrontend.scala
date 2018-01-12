package com.lb.actormodel.akka_cluster.demo.frontend

import akka.actor.{Actor, ActorRef, Terminated, _}
import akka.cluster.client.ClusterClientReceptionist
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import com.lb.actormodel.akka_cluster.demo.{BackendRegistration, JobFailed, TransformationJob, TransformationResult}
import com.typesafe.config.ConfigFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * 说明: 
  * Created by LiuBing on 2018/1/11.
  */
class ClusterFrontend extends Actor with ActorLogging{
  var backends = IndexedSeq.empty[ActorRef] //任务后台节点列表
  var jobCounter = 0

  def receive = {
    case job: TransformationJob if backends.isEmpty =>  //目前暂无执行任务节点可用
      sender() ! JobFailed("Service unavailable, try again later", job)

    case job: TransformationJob => //执行相应任务
      jobCounter += 1
      implicit val timeout = Timeout(5 seconds)
      val backend = backends(jobCounter % backends.size) //根据相应算法选择执行任务的节点
      println(s"the backend is ${backend} and the job is ${job}")
      val result  = (backend ? job)
        .map(x => x.asInstanceOf[TransformationResult])  // 后台节点处理得到结果
      result pipeTo sender  //向外部系统发送执行结果

    case BackendRegistration if !backends.contains(sender()) =>  // 添加新的后台任务节点
      context watch sender() //监控相应的任务节点
      backends = backends :+ sender()

    case Terminated(a) =>
      backends = backends.filterNot(_ == a)  // 移除已经终止运行的节点
  }
}

object  ClusterFrontend extends  App{
  val port = if (args.isEmpty) "2551" else args(0)
  val config = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$port").
    withFallback(ConfigFactory.load("frontend.conf"))

  val system = ActorSystem("liub", config)
  val frontend = system.actorOf(Props[ClusterFrontend], name = "frontend")
  ClusterClientReceptionist(system).registerService(frontend)
}