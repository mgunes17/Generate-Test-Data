package com.testdata

/**
  * Created by mgunes on 19.08.2016.
  */
class Parser(input: String) {
  def parse(): CommandSet = {
    var arr: Array[String] = input.split(" ")
    new CommandSet(arr(0), arr.tail.toList)
  }
}

case class CommandSet(val command: String, val parameters: List[String])
