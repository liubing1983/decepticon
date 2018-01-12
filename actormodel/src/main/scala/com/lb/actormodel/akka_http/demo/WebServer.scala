package com.lb.actormodel.akka_http.demo

/**
  * 说明: 
  * Created by LiuBing on 2017/12/13.
  */
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import scala.io._

object WebServer {
  def main(args: Array[String]) {

//    implicit val system = ActorSystem("my-system")
//    implicit val materializer = ActorMaterializer()
//
//    // 这个在最后的 future flatMap/onComplete 里面会用到
//    implicit val executionContext = system.dispatcher
//
//    val route =
//      path("hello") {
//        get {
//          complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Say hello to akka-http</h1>"))
//        }
//      }
//
//    val bindingFuture = Http().bindAndHandle(route, "localhost", 8080)
//
//    println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")
//    StdIn.readLine() // 等用户输入 RETURN 键停跑
//    bindingFuture
//      .flatMap(_.unbind()) // 放开对端口 8080 的绑定
//      .onComplete(_ => system.shutdown()) // 结束后关掉程序
  }
}
