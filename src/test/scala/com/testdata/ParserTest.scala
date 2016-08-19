package com.testdata

import org.scalatest.{FunSuite, BeforeAndAfter}

/**
  * Created by mgunes on 19.08.2016.
  */
class ParserTest extends FunSuite with BeforeAndAfter{
  var input: String = _
  var commandSet: CommandSet = _

  before {
    input = "insert 1000"
    commandSet = new Parser(input).parse
  }

  test("command is insert ") {
    assert(commandSet.command == "insert")
  }

  test("head parameter is 1000") {
    assert(commandSet.parameters.head == "1000")
  }
}
