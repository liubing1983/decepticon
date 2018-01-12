package com.lb.spark2.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 说明: 
  * Created by LiuBing on 2017/11/16.
  */
object HelloworldWindow {
  def main(args: Array[String]) = {
    val sparkConf = new SparkConf().setAppName("NetworkWordCount-Window")

    // 本地运行
    System.setProperty("hadoop.home.dir", "D:\\hadoop\\")
    sparkConf.setMaster("local[2]")

    // 每隔10秒处理数据
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    // 绑定监听的端口
    val lines = ssc.socketTextStream("109.254.2.175", 9999)

    // 执行wodcount操作
    val words = lines.flatMap(_.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)

    words.print()
    lines.saveAsTextFiles("d://aaaaaa/lllbb.txt")
    // 启动
    ssc.start()
    ssc.awaitTermination()
  }
}
