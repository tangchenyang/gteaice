package com.demo

import java.lang.{String => JString}
object Implicit {
  def main(args: Array[String]): Unit = {
    import MyImplicit._
    println(Math.max(1,"2"))

    "hello world".print
    println("hello world".counts)
    new JString("create by java String").print

    val a: Int = 1
    val b: Int = 2
    val c = a + b
    val d = a.+(b)

  }
}



