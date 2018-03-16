package com.lb.akkacluster

import akka.actor.ActorRef

/**
  * 说明: 
  * Created by LiuBing on 2018/2/13.
  */
final case class TransformationJob(text: String, ref : ActorRef) // 任务内容
final case class TransformationResult(text: String) // 执行任务结果
final case class JobFailed(reason: String, job: TransformationJob) //任务失败相应原因
case object BackendRegistration // 后台具体执行任务节点注册事件
case object End    // 任务执行完成