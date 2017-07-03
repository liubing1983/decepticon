package com.lb.spark.appstore

import org.apache.commons.lang.StringUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by samsung on 2017/6/30.
  */
object Test {

  def main(args: Array[String]): Unit ={

    val sparkConf = new SparkConf().setAppName("CombineTextInputFormat Demo")
    //sparkConf.setJars(List(SparkContext.jarOfClass(this.getClass).getOrElse("")))
    System.setProperty("hadoop.home.dir", "D:\\hadoop\\")
    sparkConf.setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.parallelize(
      List(

        (1, Array("0", "a", "22")),
        (1, Array("1", "a", "11")),
        (1, Array("0", "a", "22")),
        (1, Array("1", "b", "33")),
        (1, Array("1", "", "44")),
        (1, Array("1", "", "44")),
        (1, Array("1", "", "44")),
        (1, Array("1", "", "44")),
        (1, Array("1", "", "44")),
        (1, Array("1", "", "44")),
        (1, Array("1", "c", "55"))
      )
    )

    //data.aggregateByKey(1)(seq, comb).collect

    def seq(a:Array[String], b:Array[String]) : Array[String] ={
      val a1: Int = a(0).toInt + b(0).toInt
      val a2 = s"${a(1)},${b(1)}".split(",").distinct.filter(x => StringUtils.isNotEmpty(x))
      val a3 = a(2).toLong + b(2).toLong

      Array(a1.toString, a2.mkString(","), a3.toString)
    }


    def comb(a:Array[String], b:Array[String]) : Array[String] ={
      val a1: Int = a(0).toInt + b(0).toInt

      val a2 =  s"""${a(1)},${b(1)}""".split("^").toSet

      val a3 = a(2).toLong + b(2).toLong
      println(a(1).toString+"   "+b(1).toString+"   "+a2.toString())
      Array(a1.toString, a2.mkString(","), a3.toString)
    }


    val a = rdd.aggregateByKey(Array("0", "", "0"))(seq, comb)

    a.foreach{
      case (x, y) =>
        println(x +"-" + y.mkString(","))

        println(y(1).split(",").toSet.filter(x => StringUtils.isNotEmpty(x)).foreach(println _))
        println(y(1).split(",").toSet.filter(x => StringUtils.isNotEmpty(x)).size)
    }
   // println(rdd.aggregateByKey(Array("0", "", "0"), 2)(seq, comb).collect())



  }

}
