package com.lb.actormodel.callback

import java.util.concurrent.atomic.LongAdder

/**
  * 说明: 
  * Created by LiuBing on 2018/2/22.
  */
object CallBackTest {

  var jobnum = new LongAdder


  def main(args: Array[String]): Unit = {
    for(i <- 0 to 9) {
      print(s"启动新 jobnum = ${jobnum.sum()}")
      if (jobnum.sum() < 5) {
        new Thread (new Runnable {
          override def run(): Unit = {
            jobnum.add(1)
            println(s" -  ${jobnum.sum()}")
            CallBackStart.start(stop)
          }
        }).start()
      }else{
        println("job已到最大值")
        Thread.sleep(1000 * 20)
      }
      Thread.sleep(1000 * 2)
      println("------------------------------------------------")
    }
  }

  def stop(s: String): Unit = {
    jobnum.decrement()
    println(s"任务结束:${s}, jobnum = ${jobnum.sum()}")
  }

}
