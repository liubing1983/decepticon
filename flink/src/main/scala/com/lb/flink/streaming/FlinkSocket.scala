package com.lb.flink.streaming


import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * Created by liubing on 16-9-27.
 */
object FlinkSocket {

  def main (args: Array[String]) {
    // get the execution environment
    // val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 本地执行
    val env = StreamExecutionEnvironment.createLocalEnvironment()

    val text = env.socketTextStream("localhost", 9988, '\n')
    text.print

    env.execute("Socket Window WordCount")
  }

}

