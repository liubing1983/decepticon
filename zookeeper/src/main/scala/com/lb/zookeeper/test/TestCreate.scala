package com.lb.zookeeper.test

import com.lb.zookeeper.ZkConnection
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode
import org.slf4j.{Logger, LoggerFactory}

/**
  * 说明: 
  * Created by LiuBing on 2017/10/18.
  */
object TestCreate extends App {

  val logger: Logger = LoggerFactory.getLogger(TestCreate.getClass)
  logger.info("111")
  // 创建连接
  val client: CuratorFramework = ZkConnection("adsurvey", "master", 2181).getZKConnection();
  logger.info("222")
  // 启动
  client.start();
  logger.info("333")
  // 创建临时节点
  client.create().withMode(CreateMode.EPHEMERAL).forPath("/test/test_watcher", s"test_watcher- ${System.currentTimeMillis()}".getBytes());
  logger.info("inio")
  try {
    while (true) {
      Thread.sleep(60000)
    }
  } finally {
    client.close()
  }


}
