package com.lb.book.scala_cookbook.p04

/**
  * 说明: 4.1, 4.2, 4.3
  * Created by LiuBing on 2017/10/12.
  * 1. 创建构造函数
  * 2. 测试 _$eq 语法糖
  * 3. 测试getter和setter
  */


case class PersonCase(f: String)
object  PersonCase{
  def apply(): PersonCase =  PersonCase("adf")
}

class Person(var firstName: String, var lastName: String, e: String) {

//  def this(e: String){
//    this("a", "b", c)
//  }

  def this(){
    this("a", "b","c")
  }

  def this(i: Int){
    this("a", "b","c")
  }

  def this(firstName: String){
    this(1)
  }

  var age = ""

  val a = ""
  var b = ""
  private val c = ""
  private var d = ""

  override def toString: String = s"age = $age"
}

object Person extends App {
  val p = new Person("a", "b", "c")

  // 在后台scala会默认调用_$eq方法, 这个是scala的语法糖
  p.age = "10";
  p.age_$eq("20")
  println(p)

  // 测试get和set
  // a为val, 没有setter方法
  println(p.a)
  // p.a = "a"  无法调用

  // b为var, 存在getter和setter
  println(p.b)
  p.b = "b"

  // 在伴生对象中可以访问private
  println(p.c)
  //   p.c= "pc"  同val  无setter

  // 无var和val的字段
  //  p.e  在伴生对象中也无法访问

  // case class中默认类型为val
  val pc = PersonCase("f")
  println(pc.f)
  //pc.f = "pcf"

  val pc2 = PersonCase()
}