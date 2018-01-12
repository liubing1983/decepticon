package com.lb.actormodel.akka_cluster.loadbalancing

/**
  * 说明: 
  * Created by LiuBing on 2018/1/12.
  */
object Messages {

  sealed trait MathOps

  case class Add(x: Int, y: Int) extends MathOps

  case class Sub(x: Int, y: Int) extends MathOps

  case class Mul(x: Int, y: Int) extends MathOps

  case class Div(x: Int, y: Int) extends MathOps

  sealed trait ClusterMsg

  case class RegisterBackendActor(role: String) extends ClusterMsg

}