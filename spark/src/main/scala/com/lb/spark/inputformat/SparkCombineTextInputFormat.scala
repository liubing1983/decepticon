package com.lb.spark.inputformat

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * Created by liubing on 16-10-25.
 */
object SparkCombineTextInputFormat {

  // spark-submit --master spark://cloud138:7077    --executor-memory 10g  --class com.lb.starscream.spark.input.SparkCombineTextInputFormat   starscream-1.0-SNAPSHOT.jar

  val logger: Logger = Logger.getLogger(SparkCombineTextInputFormat.getClass)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("CombineTextInputFormat Demo")
    sparkConf.setJars(List(SparkContext.jarOfClass(this.getClass).getOrElse("")))

    //sparkConf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    //sparkConf.set("spark.kryo.registrator", classOf[org.apache.hadoop.io.LongWritable].getName)

    sparkConf.setMaster("local")
    val sc = new SparkContext(sparkConf)

    val conf: Configuration  = new Configuration()
    // 设置split大小
    conf.set("mapreduce.input.fileinputformat.split.maxsize", "102400000")


    // val s = sc.textFile("hdfs://10.95.3.138:8020/user/tescomm/lb/inputformat")

    //hdfs://10.95.3.138:8020/user/tescomm/lb/inputformat
    val lines = sc.newAPIHadoopFile("hdfs://10.95.3.138:8020/user/tescomm/lb/inputformat", classOf[CombineTextInputFormat], classOf[LongWritable],  classOf[Text], conf)

    //val lines = sc.newAPIHadoopFile[LongWritable, Text, CombineTextInputFormat]("hdfs://10.95.3.138:8020/user/tescomm/lb/inputformat")
    println(s"${lines.count()}--111111111111111111-${lines.first()._2.asInstanceOf[String]}")

    val  asd = lines.collect()

    asd.foreach(println _)
  }

}
