package com.lb.datastructure.stack

case class BothScatkBean[T](bothArray : Array[T] ,var top :Int,var bottom : Int)

/**
  *  实现共享栈
  * Created by liub on 2017/1/16.
  */
class BothScatk[T] ( bothSize : Int = 1024)(implicit manifest : Manifest[T]) {

  val both = new BothScatkBean[T](new Array[T](bothSize), 0, bothSize-1 )

/**
    * 入栈操作
 *
    * @param types  栈1,2 标识
    * @param values 入栈内容
    * @return 入栈成功返回true, 失败返回false
    */
  def push(types: Int, values: T): Boolean = {
    //  根据两个栈的游标判断当前栈的剩余容量
    if(both.top-1 == both.bottom) {throw new Exception("栈满!!") ; false }
    else {
      types match {
        case 1 => {
          both.bothArray(both.top) = values; both.top += 1;
          true
        }
        case 2 => {
          both.bothArray(both.bottom) = values; both.bottom -= 1;
          true
        }
        case _ => throw new Exception("push: 栈不存在!!"); false
      }
    }
  }

  /**
    * 删除栈顶的值, 并将其返回
    * @param types
    * @return
    */
  def pop(types: Int): T = {
    types  match {
      case 1 =>  both.top -= 1 ; both.bothArray(both.top)
      case 2 =>  both.bottom += 1; both.bothArray(both.bottom)
      case  _ => throw new Exception("pop: 栈不存在!!")
    }
  }

  /**
    * 查看栈顶对象而不移除它
    * @param types
    * @return
    */
  def  peek(types: Int) : T ={
    types  match {
      case 1 =>  both.bothArray(both.top-1)
      case 2 =>  both.bothArray(both.bottom+1)
      case  _ => throw new Exception("peek: 栈不存在!!")
    }
  }

  @Override
  def toString(types: Int) : String = {
    types  match {
    case 1 => {
      val a  = new  Array[T](both.top)
      Array.copy(both.bothArray, 0,  a, 0, both.top)
      a.mkString(",")
    }
    case 2 => {
      val a  = new  Array[T](bothSize - both.bottom -1)
      Array.copy(both.bothArray, both.bottom+ 1,  a, 0, bothSize - both.bottom -1)
      a.mkString(",")
    }
    case  _ => throw new Exception("toString: 栈不存在!!")
  }
  }


  /**
    * 得到当前栈的容量
    * @param types
    * @return
    */
  def size(types: Int): Int ={
    types  match {
      case 1 => both.top
      case 2 => bothSize - both.bottom - 1
      case  _ => throw new Exception("siae: 栈不存在!!")
    }
  }
}

object BothScatk {

  def main(args: Array[String]): Unit ={
  val both  = new BothScatk[String](20)
  for(i <- 0 until  10) {
    both.push(1, s"lb-top-$i")
    both.push(2, s"lb-bommon-$i")
  }

println(s"${both.size(1)}-${both.size(2)}")
    println(both.toString(1))
    println(both.toString(2))

    both.pop(1)
    both.pop(2)

    println(both.toString(1))
    println(both.toString(2))
  }
}