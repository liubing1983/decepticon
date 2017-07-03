package com.lb.megator.programming_in_scala.p09

import java.io.File

/**
  * Created by liub on 2017/1/4.
  *  使用闭包减少代码重复
  */
object FileMatcher2 {

  private def fileHere = (new File(".")).listFiles

  /**
    *  将业务逻辑剥离, 减少业务修改时 代码的修改量
    * @param matcher
    */
  def FileMatchering(matcher: String => Boolean): Unit ={
    for (file <- fileHere; if matcher(file.getName)){
      println(file.getName)
    }
  }

  def filesEnding(query: String): Unit ={
    /**
      * 将业务逻辑传递到FileMatchering
      * 减少每个方法重复代码数量
      */
    FileMatchering(_.endsWith(query))
  }

  def filesContains(query: String): Unit ={
    FileMatchering(_.contains(query))
  }

}