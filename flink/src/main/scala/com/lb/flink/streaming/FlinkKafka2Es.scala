package com.lb.flink.streaming

import java.net.InetSocketAddress
import java.util.Properties

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.elasticsearch2.{ElasticsearchSink, ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

/**
 * Created by liubing on 16-9-28.
 */
object FlinkKafka2Es {

  private val ZOOKEEPER_HOST = "localhost:2181"
  private val KAFKA_BROKER = "localhost:9092"
  private val KAFKA_GROUP = "input2"

  def main(args: Array[String]) {

    if (args.length != 3) {
      println("Usage: %s [generic options] <input path> <adinfo path> <es host> \n", getClass().getSimpleName())
      //System.exit(1)
    }

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "10.95.3.136:9092")
    properties.setProperty("zookeeper.connect", "10.95.3.136:2181")
    properties.setProperty("group.id", "lb2")

    // val stream = env.addSource(new  FlinkKafkaConsumer08("testKafka",  new SimpleStringSchema(), properties))
    val ds = env.addSource(new  FlinkKafkaConsumer08("lb2",  KafkaStringSchema, properties))
    ds.setParallelism(1).print()

   // val kafkaproducer = new FlinkKafkaProducer08[String]("10.95.3.136:9092","output",KafkaStringSchema)

    // configure Elasticsearch
    val config: java.util.Map[String, String] = new java.util.HashMap[String, String]
    config.put("bulk.flush.max.actions", "1")
    config.put("cluster.name", "lisp.elasticsearch")

    val transports = new java.util.ArrayList[InetSocketAddress]()
    transports.add(new InetSocketAddress("10.95.3.138", 9300))

    val elasticsearchSink = new ElasticsearchSinkFunction[String] {
      def createIndexRequest(element: String): IndexRequest = {
//        val json = new java.util.HashMap[String, String]
//        if (element.contains(",")) {
//          val k = element.split(",")
//          json.put("log", k(1))
//          json.put("data", k(0))
//        }
//        println("SENDING: " + element)
//        Requests.indexRequest.index("tescomm3").`type`("abc").source(json)

        println(element)
        Requests.indexRequest.index("filewatcher").`type`("filewatcher").source(element)
      }

      override def process(element: String, ctx: RuntimeContext, indexer: RequestIndexer) {
        indexer.add(createIndexRequest(element))
      }
    }

    val esSink = new ElasticsearchSink(config, transports, elasticsearchSink) with SinkFunction[String] {}

    ds.addSink(esSink)

    env.execute("Flink ElasticSearch2 Example")

  }

}
