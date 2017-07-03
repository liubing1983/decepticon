package com.lb.zookeeper.crud

import java.util.concurrent.{CountDownLatch, ExecutorService, Executors}

import com.lb.zookeeper.ZkConnection
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.api.{BackgroundCallback, CuratorEvent}
import org.apache.zookeeper.CreateMode

/**
  * Created by liub on 2017/3/10.
  */
object BackgroundDemo {

  val client: CuratorFramework = ZkConnection().getZKConnection()

  val exec: ExecutorService = Executors.newFixedThreadPool(2);
  val c: CountDownLatch = new CountDownLatch(2)

  def main(args: Array[String]): Unit = {
    client.start();

    var path, code, name = ""

    client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
      // 使用异步接口
      .inBackground(new BackgroundCallback {
      @Override
      def processResult(client: CuratorFramework, event: CuratorEvent): Unit = {
        path = event.getPath
        code = event.getResultCode.toString
        name = Thread.currentThread().getName
        c.countDown()
      }
      //  使用exec线程池
    }, exec).forPath("/a", "init".getBytes)
    println(s"${path} - ${code} - ${name}")

    var path2, code2, name2 = "";
    client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
      // 使用异步接口
      .inBackground(new BackgroundCallback {
      override def processResult(client: CuratorFramework, event: CuratorEvent): Unit = {
        path2 = event.getPath
        code2 = event.getResultCode.toString
        name2 = Thread.currentThread().getName
      }
    }).forPath("/a", "init".getBytes)
    println(s"${path2} - ${code2} - ${name2}")
    exec.shutdown()
  }
}
