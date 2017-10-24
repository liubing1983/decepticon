package com.lb.zookeeper.watcher

import com.lb.zookeeper.ZkConnection
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.{PathChildrenCache, PathChildrenCacheEvent, PathChildrenCacheListener}
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent._

/**
  * Created by liub on 2017/3/12.
  */
object ChildrenNodeCache {

  val client : CuratorFramework = ZkConnection("appstore-watcher", "180.76.139.166", 2181).getZKConnection();

  def main(args: Array[String]): Unit = {
    client.start()

    val cache : PathChildrenCache = new PathChildrenCache(client, "/", true)

    cache.start();

    cache.getListenable.addListener(new PathChildrenCacheListener {
      override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
        event.getType match {
          case  Type.CHILD_ADDED => println(s"add: ${event.getData.getPath} - ${new String(event.getData.getData)}")
          case  Type.CHILD_UPDATED => println(s"update: ${event.getData.getPath} - ${new String(event.getData.getData)}")
          case  Type.CHILD_REMOVED => println(s"delete: ${event.getData.getPath} - ${new String(event.getData.getData)}")
          case  _ => println(s"other: ${event.getData.getPath} - ${new String(event.getData.getData)}")
        }
      }
    })

    while (true){
      println("123")
      Thread.sleep(10000)
    }
  }


}
