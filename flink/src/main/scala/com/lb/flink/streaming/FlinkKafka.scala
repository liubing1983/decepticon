package com.lb.flink.streaming

import java.util.Properties

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
import org.apache.flink.streaming.util.serialization.{DeserializationSchema, SerializationSchema}

/**
 * Created by liubing on 16-9-27.
 */
object FlinkKafka {

  def main(args: Array[String]): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "10.95.3.136:9092")
    properties.setProperty("zookeeper.connect", "10.95.3.136:2181")
    properties.setProperty("group.id", "testKafka1")

   // val stream = env.addSource(new  FlinkKafkaConsumer08("testKafka",  new SimpleStringSchema(), properties))
    val stream = env.addSource(new  FlinkKafkaConsumer08("lb2",  KafkaStringSchema, properties))
    stream.setParallelism(4).print()

    env.execute("FlinkKafkaStreaming")

  }

}

object KafkaStringSchema extends SerializationSchema[String] with DeserializationSchema[String] {

  import org.apache.flink.api.common.typeinfo.TypeInformation
  import org.apache.flink.api.java.typeutils.TypeExtractor

  override def serialize(t: String): Array[Byte] = t.getBytes("UTF-8")

  override def isEndOfStream(t: String): Boolean = false

  override def deserialize(bytes: Array[Byte]): String = new String(bytes, "UTF-8")

  override def getProducedType: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
}
