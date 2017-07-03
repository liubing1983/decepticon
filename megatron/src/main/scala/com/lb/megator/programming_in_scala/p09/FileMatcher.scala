package com.lb.megator.programming_in_scala.p09

import java.io.File

/**
  * Created by liub on 2017/1/4.
  */
object FileMatcher {

  private def fileHere = (new File(".")).listFiles

  // 传统写法, 业务逻辑大致相同, 重复代码量比较大
  def filesEnding(query: String): Unit ={
    for (file <- fileHere; if file.getName.endsWith(query)){
      println(file.getName)
    }
  }


  def filesContains(query: String): Unit ={
    for (file <- fileHere; if file.getName.contains(query)){
      println(file.getName)
    }
  }

  def main(args: Array[String]): Unit ={
    filesEnding(".")
  }

}
