package com.lb.zookeeper.crud

import com.lb.zookeeper.ZkConnection
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.data.Stat
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by liub on 2017/3/10.
  */
object CuratorCrudDemo {

   val logger: Logger =  LoggerFactory.getLogger(CuratorCrudDemo.getClass)

  // 创建连接
  val client : CuratorFramework = ZkConnection().getZKConnection();

  def  crudDemo() {
    // 启动
    client.start();

    try {
      logger.info("开始创建节点");
      // 创建节点
      client.create().forPath("/a");
      // 创建节点， 并附加初始值
      client.create().forPath("/b", "init_b".getBytes());
      // 创建临时节点
      client.create().withMode(CreateMode.EPHEMERAL).forPath("/c");
      // 创建临时节点， 并附加初始值, 自动同时递归创建父节点
      client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/d", "init_d".getBytes());
      // 读取子节点
      logger.info("读取节点数据");
      val stat: Stat = new Stat();

      println("a:"+new String(client.getData().storingStatIn(stat).forPath("/a")));
      println("b:"+new String(client.getData().storingStatIn(stat).forPath("/b")));
      println("c:"+new String(client.getData().storingStatIn(stat).forPath("/c")));
      println("d:"+new String(client.getData().storingStatIn(stat).forPath("/d")));
      println(stat.getVersion());

      Thread.sleep(10000)

      logger.info("修改节点数据");
      client.setData().withVersion(stat.getVersion()).forPath("/a", "update_a".getBytes()).getVersion();
      client.setData().withVersion(stat.getVersion()).forPath("/b", "update_b".getBytes()).getVersion();
      client.setData().withVersion(stat.getVersion()).forPath("/c", "update_c".getBytes()).getVersion();
      client.setData().withVersion(stat.getVersion()).forPath("/d", "update_d".getBytes()).getVersion();

      println("a:"+new String(client.getData().storingStatIn(stat).forPath("/a")));
      println("b:"+new String(client.getData().storingStatIn(stat).forPath("/b")));
      println("c:"+new String(client.getData().storingStatIn(stat).forPath("/c")));
      println("d:"+new String(client.getData().storingStatIn(stat).forPath("/d")));
      println(stat.getVersion());
      //Thread.sleep(30000);
      logger.info("删除节点");
      client.delete().deletingChildrenIfNeeded().withVersion(stat.getVersion()).forPath("/a");
      client.delete().deletingChildrenIfNeeded().withVersion(stat.getVersion()).forPath("/b");

      client.close()
    } catch {
      case e : Exception => e.printStackTrace()
    }
  }

  def main(Args: Array[String]): Unit ={
    CuratorCrudDemo.crudDemo()
  }
}