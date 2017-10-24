package com.lb.zookeeper

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry

/**
  * Created by liub on 2017/3/10.
  */
case class ZkConnection(namespace: String = "lb", ip: String = "cloud136,cloud138,cloud139,hd5,hd6,hd7,hd8", port: Int = 2181) {

  def getZKConnection(): CuratorFramework = {
    CuratorFrameworkFactory.builder()
      // 连接IP
      .connectString(ip)
      // 超时时间
      .sessionTimeoutMs(50000).
      // 重试策略 (初始sleep时间, 最大重试次数)
      retryPolicy(new ExponentialBackoffRetry(1000, 5))
      //重试策略 (初始sleep时间, 最大重试次数, 最大sleep时间)
      // retryPolicy(new ExponentialBackoffRetry(100, 3, 5000))
      // 命名空间, 可以省略
      .namespace(namespace).build();
  }
}
