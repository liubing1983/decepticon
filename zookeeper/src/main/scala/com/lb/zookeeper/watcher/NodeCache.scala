package com.lb.zookeeper.watcher

import com.lb.zookeeper.ZkConnection
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.{NodeCache, NodeCacheListener}

/**
  * 监控zookeeper节点状态
  * Created by liub on 2017/3/11.
  */
object NodeCacheTest {
  // 创建连接
  val client : CuratorFramework = ZkConnection().getZKConnection();

  def main(args: Array[String]): Unit = {
    client.start()
    val cache: NodeCache = new NodeCache(client, "/a", false)
    cache.start(true)

    cache.getListenable().addListener(new NodeCacheListener {
      override def nodeChanged(): Unit = {
        // 监控方式1
        // 在内部类内部调用, 无法监控节点删除
        //println(cache.getCurrentData.getStat.getVersion)
        //println(new String(cache.getCurrentData.getData))
      }
    })


    // 持续获取节点状态
    while (true) {
      try {
        // 监控方式2
        println(new String(cache.getCurrentData.getData))
      } catch {
        // 通过空指针异常判断节点是否存在
        case e: NullPointerException => println("节点不存在")
      }
      //println(s"${System.currentTimeMillis()}-111111111111111")
      Thread.sleep(1000)
    }

  }
}
