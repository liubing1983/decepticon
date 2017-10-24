package com.lb.megator.reflect

import java.nio.file.attribute.UserDefinedFileAttributeView

import scala.beans.{BeanDescription, BeanProperty}

class User {
  @BeanProperty var name: String = "";
  @BeanProperty var age = 0;
}

/**
  * Created by samsung on 2017/7/11.
  */
object TestReflect {

  def main(args: Array[String]): Unit = {

    val c = Class.forName("com.lb.megator.reflect.User")


    println(c.getName)

    val user = c.getFields
    c.getMethods.foreach(println)

    val method = c.getDeclaredMethod("setName", classOf[String])
    val aaa = method.invoke(c.newInstance(), "5")
    // 如果是Int的话,要用new Integer(5)

    user.foreach(println)
method.getParameters.foreach(println)
       println(method.getName+"-------------------")
    //println(c2.getCanonicalName)


    val u   = c.newInstance()
    u.asInstanceOf[User].setName("lb")
    println(u.asInstanceOf[User].getName+"============")

    //println(u.lb("123"))
  }

}
