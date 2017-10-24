package com.lb.book.scala_cookbook.p04

/**
  * 说明: 
  * Created by LiuBing on 2017/10/13.
  */
class Employee(name : String, age : Int, address: String)  extends Person2(name, age){



}

object  Employee extends App{
  val a = new Employee("liub", 34, "")
  println(a)
}

class Person2(name : String, age : Int){
  override def toString: String = s"$name  --  $age"
}
