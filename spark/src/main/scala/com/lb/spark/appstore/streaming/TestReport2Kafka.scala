package com.lb.spark.appstore.streaming



import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.reflect.ClassTag


/**
  * Created by samsung on 2017/5/26.
  */
object TestReport2Kafka {

  def main(args: Array[String]): Unit ={

    val Array(brokers, topics) = args

    val sparkConf = new SparkConf().setAppName("report Demo")
    // 本地运行
    System.setProperty("hadoop.home.dir", "D:\\hadoop\\")
    sparkConf.setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // 设置检查点, 防止程序异常
    ssc.checkpoint("D:\\hadoop\\2\\")

    // 需要接受的topic列表
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map("metadata.broker.list" -> "109.254.2.175:9092", "auto.offset.reset" ->"smallest")

    val topicLines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    topicLines.map{lines =>
      println("===============================================================")
      (lines._1+"----"+lines._2)
    }.print()

    ssc.start()
    ssc.awaitTerminationOrTimeout(1000 * 60 * 10)

  }

}
