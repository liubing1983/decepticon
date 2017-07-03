package com.lb.spark.distinct_crm

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by samsung on 2017/5/2.
  */
object JoinCrmData {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("CombineTextInputFormat Demo")
    //sparkConf.setJars(List(SparkContext.jarOfClass(this.getClass).getOrElse("")))
    System.setProperty("hadoop.home.dir", "D:\\hadoop\\")
    sparkConf.setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val crmdata = sc.textFile("E:\\公司资料\\数据映射\\lb-crm-distince.txt")
    val tagfile = sc.textFile("E:\\公司资料\\数据映射\\lb-whole.txt")

  val t1 = tagfile.filter(tagFilter(_)).map{ line=>
      val lines = line.split("\t", -1)
      (lines(221), subString(lines, 8))
    }.groupByKey
//
////t1.saveAsTextFile("E:\\公司资料\\数据映射\\tag")
//    println(t1.count()+"-----------------------====================================================-----------")
    // 打印所有不符合规则的数据, 测试后发现主要问题为数据列数存在问题
//    val t3 = tagfile.filter(!tagFilter(_))
//    println(t3.count()+"-----------------------====================================================-----------")
//    t3.saveAsTextFile("E:\\公司资料\\数据映射\\error")
//
//    val t3 =  crmdata.filter(_.split("\t", -1).size == 4)
//    println(t3.count()+"==================================================================================888888888888888")

    crmdata
      //过滤数据 第6列为1 且 总列数为49
      .filter(_.split("\t", -1).size == 49).map{ line =>
      val lines = line.split("\t", -1)
      (lines(1), subString(lines, 5))
    }.groupByKey().join(t1).map{ case (k, (v1, v2)) =>s"${v1.head._1}${"\t"}${v2.head._1}${"\t"}${v1.head._2}${"\t"}${v2.head._2}"}.saveAsTextFile("E:\\公司资料\\数据映射\\lb")

  }

  /**
    * 拆分字符串 根据规则将字符串拆分成2段, 返回一个元组
    * @param s
    * @param index
    * @return
    */
  def subString(s: Array[String], index: Int) : (String, String) ={
    ((for(i <- 0 to index) yield s"${s(i)}").mkString("\t"),  (for(i <- index+1 until s.length if(i != 221))  yield s"${s(i)}").mkString("\t"))
  }

  /**
    * 数据过滤条件
    * @param s
    * @return
    */
  def crmFilter(s: String) : Boolean ={
    val x = s.split("\t", -1)
    if(x(5) == 1 && x.size==49) true
    else false
  }

  /**
    * tag标签过滤条件
    * @param s
    * @return
    */
  def tagFilter(s: String) : Boolean ={
    val x = s.split("\t", -1)
    if(x.size==223 ) {
      if(x(221).size == 11) true else {println(x(221));false}
    } else {
      println(x.size)
      false
    }
  }
}