package com.lb.spark.wd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

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


    val r1 = sc.parallelize(List(("a",  Array(1,2,3)), ("a", Array(1,2,3)), ("c", Array(3,4,5))))
    r1.map{ line =>
          for(i <-1 to 10) yield{
            (line._1+ i, line._2)
          }
    }.flatMap(x => x).reduceByKey((x, y) => (x, y).zipped.map(_ + _)).map{case (k, v) => (k, v.mkString(","))}.collect().foreach(println _)
    sc.stop()
  }
}
