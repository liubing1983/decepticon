package com.lb.actormodel.akka

import akka.actor._
import akka.routing.RoundRobinPool
import akka.pattern.ask
import akka.util.Timeout
import scala.language.postfixOps
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
  * 说明: 
  * Created by LiuBing on 2018/1/10.
  */
class HelloActorRouter extends Actor with  ActorLogging{

  override def receive: Receive = {
    case s => log.info(s"s====${s}")
      //Thread.sleep(1000 * 4)
      sender() ! context.system.name
  }

}

object  HelloActorRouter extends  App{

  implicit val timeout = Timeout(5 seconds)

  val system = ActorSystem()
  val workerRouter: ActorRef =
    system.actorOf(Props.create(classOf[HelloActorRouter]).withRouter(new RoundRobinPool(8)), "123")

  for(i <- 1 to 100){
    val a =  workerRouter ? i
     a.onSuccess{
       case a => println(a.toString)
     }
  }

  system.terminate()
}
