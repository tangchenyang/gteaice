package com.demo

object Implicit {
  def main(args: Array[String]): Unit = {
    import MyImplicit._
    println(Math.max(1,"2"))

    "hello world".print
    println("hello world".counts)

  }
}



