package com.lb.megator.programming_in_scala.p04

/**
  * Created by liub on 2016/12/29.
  */
class TestSingleton {

  val testVal = 0

  var testVar = 0

  private var testVarP = 0

  def add(b: Byte) {testVar +=b }

}

object TestSingleton{
  val t = new TestSingleton
  // 可以访问TestSingleton中的私有成员变量
  t.testVarP = 4
}

class TestSinglentonPrivate{
  val t = new TestSingleton
  // 无法访问TestSingleton中的私有成员变量
  // t.testVarP = 4
}
