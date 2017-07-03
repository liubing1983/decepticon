package com.lb.megator.test

/**
  * Created by liub on 2016/12/29.
  */
object TestMatch {


  /**
    * 计算兔子队列  循环方式
    * @param i
    */
  def abc(i: Int): Unit ={
    var a: BigDecimal = 0
    var b: BigDecimal = 1
    var c: BigDecimal = 1

    for(_ <- 0 to i){
    c = a+ b
    a= b
    b = c
  }
  println(c)
}


  /**
    * 计算兔子队列
    * @param i  循环次数
    * @param d  当前次数
    * @param a  向量A
    * @param b  向量B
    * @return
    */
  def abcd(i: Long, d: Long=0, a: BigDecimal = 0, b: BigDecimal = 1): BigDecimal ={
      val qwe = System.currentTimeMillis()
      println(qwe)
      //
      if( d>= i)  a+b
      else abcd(i,d+1 , b, a+b)
  }

  def main(args: Array[String]): Unit ={
    val t1 = System.currentTimeMillis()
   // abc(1000000)
    //  1千万循环 28m
    println(System.currentTimeMillis() - t1)

    val t2 = System.currentTimeMillis()
    println(abcd(1000000))
    // 1千万循环  27.44m  (1646884)
    println(System.currentTimeMillis() - t2)

     List("1,1", "aaaaa", "a,b,c").map{ x=>
       x.split(",", -1) match {
         case Array(x, y) => println(s"$x - $y"); (x, y)
         case _ => (0,0)
       }
     }.distinct.map{
       case(x: String , y :String) =>  println(x)
       case _ => println(0)
     }
  }

}
