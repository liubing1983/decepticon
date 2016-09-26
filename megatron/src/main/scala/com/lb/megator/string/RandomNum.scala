package com.lb.megator.string

/**
 * Created by liubing on 16-9-26.
 */
object RandomNum {

  def main(args: Array[String]): Unit = {
    val r = scala.util.Random

    // 生成一个随机ｉｎｔ
    println(r.nextInt())

    //　生成一个随机ｌｏｎｇ
    r.nextLong()

    //生成一个随机字符
    r.nextPrintableChar()

    // 设置随机数范围
    println(r.nextInt(50))


    /**
     * 设置随机数种子
     * 设置种子后相同次数的随机数相同
     */
    r.setSeed(100)
    for (i <- 0 to 10) {
      println(r.nextInt(100))
    }

    //创建一个随机长度的ｒａｎｇｅ
    var range = 0 to r.nextInt(10)
    println(range)

  }

}
