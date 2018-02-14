package com.lb.megator.io

import java.io.File

import scala.collection.mutable


/**
  * 说明: 
  * Created by LiuBing on 2018/1/30.
  */
object Directory {

  val hset  = mutable.HashSet("eml","msg","pst")

  hset.contains("eml")

  def main(args: Array[String]): Unit = {
    println(hset.contains("eml"))
    println(hset.contains("emL".toLowerCase))

    dir("e://workspace")
  }

  def dir(path: String): Unit = {
    new File(path).listFiles().foreach {
      // 处理文件
      case x if (x.isFile) => println(s"文件: ${x.getPath}-${x.getPath.split("\\.", -1).last}")
      // println(x.getPath)
      // x.getPath.split("\\.", -1).foreach(println)
      // 处理目录
      case x if (x.isDirectory) => println(s"目录:  ${x.getPath}"); dir(x.getPath)
      case _ => println(s"其他")
    }
  }


  def subdirs2(dir: File): Iterator[File] = {
    val d = dir.listFiles.filter(_.isDirectory)
    val f = dir.listFiles.filter(_.isFile).toIterator
    f ++ d.toIterator.flatMap(subdirs2 _)
  }

}
