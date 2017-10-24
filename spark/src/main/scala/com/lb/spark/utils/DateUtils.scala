package com.lb.spark.utils

import java.text.SimpleDateFormat

/**
  * Created by samsung on 2017/6/27.
  */
object DateUtils {

  implicit class LbDateUtils(val s: Long) {
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    def dateFormat: String = sdf.format(s)
  }

  def main(args: Array[String]): Unit ={
    val a = 1500002421000L

    println(a.dateFormat)
    println(a.dateFormat.substring(0, 10))
    println(a.dateFormat.substring(11, 13))
  }

}
