package com.lb.spark.appstore.streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Created by samsung on 2017/5/31.
  */
object TestReport2Kafka2Zk {

  def main(args: Array[String]): Unit ={

    val Array(brokers, topics, group) = args

    val sparkConf = new SparkConf().setAppName("report Demo")
    // 本地运行
    System.setProperty("hadoop.home.dir", "D:\\hadoop\\")
    sparkConf.setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // 设置检查点, 防止程序异常
    ssc.checkpoint("D:\\hadoop\\2\\")

    // 需要接受的topic列表
    val topicsSet = topics.split(",").toSet

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "latest",  //largest表示接受接收最大的offset(即最新消息),smallest表示最小offset,即从topic的开始位置消费所有消息
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    //val kafkaParams = Map("metadata.broker.list" -> "109.254.2.175:9092", "auto.offset.reset" ->"smallest")

   // val topicLines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
//   val topicLines = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams, 0))
//    topicLines.map{lines =>
//      println("===============================================================")
//      (lines._1+"----"+lines._2)
//    }.print()

    ssc.start()
    ssc.awaitTerminationOrTimeout(1000 * 60 * 10)

  }

}
