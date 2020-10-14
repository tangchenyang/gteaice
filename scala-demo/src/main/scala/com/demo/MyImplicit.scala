package com.demo

object MyImplicit {
  implicit def strToInt(str: String) = str.toInt
  implicit def strToInt1(str: String) = new MyString(str)
}


class MyString(s: String) {
  def counts = s.size
  def print= System.err.println(s)
}