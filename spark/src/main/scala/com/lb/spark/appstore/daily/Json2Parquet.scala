package com.lb.spark.appstore.daily

import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s._
import org.json4s.jackson.JsonMethods._



/**
  * Created by samsung on 2017/6/16.
  */
object Json2Parquet {

  // spark-submit --master yarn-cluster   --class com.lb.spark.appstore.daily.Json2Parquet  spark-1.0-SNAPSHOT.jar

  def main(args : Array[String]): Unit ={
    val sparkConf = new SparkConf().setAppName("Json2Parquet")
    val sc = new SparkContext(sparkConf)

    sparkConf.setMaster("local")

    //导入隐式值
    implicit val formats = DefaultFormats
    sc.textFile("hdfs://appstore-stg-namenode1:8020/user/cheil/out-5/action").map{

      line => println(line)
        line
    }.first()
  }
}
