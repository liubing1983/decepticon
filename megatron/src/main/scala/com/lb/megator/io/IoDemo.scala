package com.lb.megator.io

import scala.io.{BufferedSource, Source}

/**
  * 说明: 
  * Created by LiuBing on 2018/1/15.
  */
object IoDemo extends App {


  Source.fromFile("E:\\examples").foreach{x =>
    println(x.toString)
  }

  System.exit(0)

  val source= Source.fromURL("http://blog.csdn.net/w_j_w2010/article/details/50263329","UTF-8")

  val lineIterator =source.getLines
  for(l<-lineIterator){
    println(l.toString())
  }
}
