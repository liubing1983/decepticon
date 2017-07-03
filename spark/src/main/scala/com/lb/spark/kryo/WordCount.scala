package com.lb.spark.kryo

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liub on 2016/12/14.
  */
object WordCount {

  // spark-submit --master spark://cloud138:7077    --executor-memory 10g  --class com.lb.spark.wd.WordCount

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: <input path> <output path>")
      System.exit(1)
    }
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val line = sc.textFile(args(0))
    line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).saveAsTextFile(args(1))
    sc.stop()
  }
}
