package com.lb.megator.kxscala.ch08

/**
 * 快学scala第八章练习  -- 1
 * Created by liubing on 16-11-1.
 */
class  BankAccount (initialBalance: Double) {

  private var  balance = initialBalance

  def deposit(amount: Double) = {balance += amount; balance}
  def withdraw(amount: Double)= {balance -= amount; balance}
}


object CheckingAccount extends BankAccount(1.0){

  def main(args: Array[String]): Unit ={

    println(deposit(100))
  }
}


class SavingsAccount(initialBalance:Double) extends BankAccount(initialBalance){

  private  var num = 0


  override  def deposit(amount: Double) = {
    num match {
      case n:Int if (n > 3) => num += 1; amount
      case _ => amount + 1
    }
  }
}
