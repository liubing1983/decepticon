package com.lb.payuser

import java.io.File

import scala.collection.mutable
import scala.io.Source

/**
  * Created by samsung on 2017/4/19.
  */
object PayuserEtl {

  /**
    * 根据imei匹配
    */
  def imei(): Unit = {
    val map = new mutable.HashMap[String, String]()
    for (file <- (new File("E:\\rtdata\\20170419")).listFiles) {
      Source.fromFile(file, "UTF-8").getLines().foreach { line =>
        val lines = line.split("\t")
        map.put(lines(4), "")
      }
    }
    println("imei匹配总数： " + map.size)
  }

  /**
    * 根据电话号码匹配
    */
  val telmap = new mutable.HashMap[(String, String), String]()

  def tel(path: String): Unit = {
    for (file <- (new File(s"E:\\rtdata\\${path}")).listFiles) {
      Source.fromFile(file, "UTF-8").getLines().foreach { line =>
        val lines = line.split("\t")
        // 判断电话号码是否大于11位
        if (lines(3).size > 11) {
          // 截断电话号前的86
          lines(3) = lines(3).substring(2, lines(3).size)
        }
        telmap.put((SHA256.testSha256(lines(3)), SHA256.testSha256(lines(4))), "")
      }
    }
    print(s"${path}爱贝支付总用户数：${telmap.size}.   -  ")
  }

  val shequmap = new mutable.HashMap[(String, String), String]()
  val shequmap_tel = new mutable.HashMap[String, String]()
  val shequmap_email = new mutable.HashMap[String, String]()

  def shequ(path: String): Unit = {
    Source.fromFile(new File(path), "UTF-8").getLines().foreach { line =>
      val lines = line.replaceAll("\"", "").split("\t", -1)
      //println(s"${lines(0)}${lines(1)}")
      shequmap.put((lines(0), lines(1)), "")
      shequmap_tel.put(lines(0), "")
      shequmap_email.put(lines(1), "")
    }

    println(s"三星社区用户总数： 按(电话+邮箱)-${shequmap.size},  按(电话)-${shequmap_tel.size}, 按(邮箱)-${shequmap_email.size}")
  }

  /**
    * 比对数据
    */
  def join(): Int = {
    val ss = telmap.keySet.filter(x => shequmap.getOrElse(x, "0") == "0")
    //val i1 = telmap.keySet.filter(x =>  shequmap.getOrElse(x, "0") != "0").size
    val i4 = telmap.size - ss.size
    val i2 = ss.filter(x => shequmap_tel.getOrElse(x._1, "0") != "0").size
    val i3 = ss.filter(x => shequmap_email.getOrElse(x._2, "0") != "0").size

    telmap.clear()
    print(s"电话+邮箱匹配数: ${i4}, 电话匹配数: ${i2}, 邮箱匹配数 ${i3}.  ")
    i4 + i2 + i3
  }

  def main(args: Array[String]): Unit = {
    // 计算三星社区总用户数
    shequ("E:\\rtdata\\t_gc_users.txt")

    // 待匹配的爱贝支付列表
    //val pay_path_list : List[String] = List("20170419","20170420","20170421","20170422","20170423")
    println()

    // 匹配每天的数据
   // List("20170421", "20170422", "20170423", "20170424", "20170425", "20170426", "20170427", "20170429", "20170430", "20170501",  "20170502").foreach { x =>
      List("20170503", "20170504", "20170505", "20170506", "20170507", "20170508", "20170509").foreach { x =>
      tel(x)
      println(s"总数: ${join}")
    }

  }
}