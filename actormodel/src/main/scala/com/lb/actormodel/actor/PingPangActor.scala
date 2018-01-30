package com.lb.actormodel.actor

import akka.actor._

import scala.util.Random

/**
  * 说明: 模拟打乒乓球, 谁先得到15分, 比赛结束
  * Created by LiuBing on 2018/1/23.
  */
trait Message

case class PingPangMessage(i: Int) extends Message

case class StartSystemMessage() extends Message

case class StartMessage(actorRef: ActorRef) extends Message

case class StopMessage(msg: String) extends Message

class PingActor extends Actor {
  override def receive: Receive = {
    case StartMessage(ref: ActorRef) =>println("ping 发球"); ref ! PingPangMessage(Random.nextInt(30))
    case PingPangMessage(i) if (i == Random.nextInt(30)) =>  context.actorSelection("/user/systemstart") ! StopMessage("ping")
    case PingPangMessage(i) => sender ! PingPangMessage(i)
    //case _ => println("error  ping")
  }

  override def unhandled(message: Any): Unit = {
    println(message)
  }
}

class PangActor extends Actor {
  override def receive: Receive = {
    case StartMessage(ref: ActorRef) =>println("pang 发球"); ref ! PingPangMessage(Random.nextInt(30))
    case PingPangMessage(i) if (i == Random.nextInt(30)) =>  context.actorSelection("/user/systemstart") ! StopMessage("pang")
    case PingPangMessage(i) => sender ! PingPangMessage(i)
    // case _ => println("error  pang")
  }

  override def unhandled(message: Any): Unit = {
    println(message)
  }
}

class StartActor extends Actor {

  val ping = context.actorOf(Props[PingActor], "ping")
  val pang = context.actorOf(Props[PangActor], "pang")

  // 存储比分
  var(pingNum:Int, pangNum:Int)= (0,0)

  override def preStart(): Unit = {
    println("system statrt")
  }

  override def receive: Receive = {
    case StartSystemMessage() => ping ! StartMessage(pang)
    case StopMessage(s:String) if((pingNum >=15 || pangNum >=15) && Math.abs(pangNum-pingNum) >1) =>{
      println(s"最终得分 ${pingNum} : ${pangNum}")
      StartActor.shutdown
    }
    case StopMessage("ping") => {
      pangNum = pangNum + 1
      println(s"${pingNum} : ${pangNum}")
      ping ! StartMessage(pang)
    }
    case StopMessage("pang") =>
      pingNum = pingNum + 1
      println(s"${pingNum} : ${pangNum}")
      pang ! StartMessage(ping)
  }
}

object StartActor {
  var ref: ActorRef = _
  var sys: ActorSystem = _

  def create: Unit = {
    sys = ActorSystem("pingpang")
    ref = sys.actorOf(Props[StartActor], "systemstart")
  }

  def getRef = ref

  def shutdown = {
    sys.terminate()
  }
}


object PingPangActor extends App {
  StartActor.create
  StartActor.getRef ! StartSystemMessage()
}
