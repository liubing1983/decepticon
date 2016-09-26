package com.lb.megator.string

/**
 * Created by liubing on 16-9-26.
 */
object ReplaceTest {

  def main(args: Array[String]): Unit ={
    // 在字符串中使用变量
    val name = "lb"

    println(s"name = $name")


    // 格式化字符串,　保留两位小数
    val age = 200
    println(f"$age%.2f")


    // 不对字符串中的转义字符转义
    println(raw"ac\nmilan")
    //对比
    println(s"ac\nmilan")
  }

}
