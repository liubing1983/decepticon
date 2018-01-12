package com.lb.actormodel.akka_cluster.demo.client

import akka.actor.{Actor, ActorLogging, ActorPath, ActorSystem, Props}
import akka.cluster.client.{ClusterClient, ClusterClientSettings}
import akka.pattern.Patterns
import akka.util.Timeout
import com.lb.actormodel.akka_cluster.demo.{TransformationJob, TransformationResult}
import com.typesafe.config.ConfigFactory

import scala.actors.threadpool.AtomicInteger
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.StdIn
import scala.util.{Failure, Success}

/**
  * 说明: 
  * Created by LiuBing on 2018/1/11.
  */
case class Send(count: Int)

class ClusterClientLiub extends Actor with ActorLogging {
  val initialContacts = Set(
    ActorPath.fromString("akka.tcp://liub@127.0.0.1:2551/system/receptionist"))
  val settings = ClusterClientSettings(context.system).withInitialContacts(initialContacts)

  val c = context.system.actorOf(ClusterClient.props(settings), "demo-client")


  def receive = {
    case TransformationResult(result) => {
      println(s"Client response and the result is ${result}")
    }
    case Send(counter) => {
      val job = TransformationJob("hello-" + counter)
      implicit val timeout = Timeout(5 seconds)
      val result = Patterns.ask(c, ClusterClient.Send("/user/frontend", job, localAffinity = true), timeout)

      result.onComplete {
        case Success(transformationResult) => {
          self ! transformationResult
        }
        case Failure(t) => println("An error has occured: " + t.getMessage)
      }
    }
  }
}

object DemoClient {
  def main(args: Array[String]) {

    //    TransformationFrontendApp.main(Seq("2551").toArray)  //启动集群客户端
    //    TransformationBackendApp.main(Seq("8001").toArray)   //启动三个后台节点
    //    TransformationBackendApp.main(Seq("8002").toArray)
    //    TransformationBackendApp.main(Seq("8003").toArray)

    val system = ActorSystem("liub222", ConfigFactory.load("client.conf"))
    val ref = system.actorOf(Props[ClusterClientLiub], name = "clientJobTransformationSendingActor")

    val counter = new AtomicInteger
    import system.dispatcher
    for (i <- 1 to 100) {
      ref ! Send(i)
    }


    StdIn.readLine()
    system.terminate()
  }
}

