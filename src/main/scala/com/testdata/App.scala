package com.testdata

import com.datastax.driver.core._
import com.datastax.driver.core.exceptions.InvalidQueryException
import com.typesafe.config.ConfigFactory

/**
  * Created by mgunes on 11.08.2016.
  */
object App {
  var testTable: TestTable = _

  def main (arguments: Array[String]): Unit = {
    val connectionInfo : ConnectionInfo = new App().readConnectionInfo()

    val builder = Cluster.builder().addContactPoint(connectionInfo.address)
    val cluster = builder.build()
    val session = cluster.connect(connectionInfo.keysapce)

    val input: String = scala.io.StdIn.readLine("test> ")
    val commandSet: CommandSet = new Parser(input).parse

    try {
      processCommand(commandSet, session)
      session.close
      System.exit(0)
    } catch {
      case ex: InvalidQueryException => {
        println("Invalid Query")
        val input: String = scala.io.StdIn.readLine("test> ")
        val commandSet: CommandSet = new Parser(input).parse
      }
      case e: Exception => {
        println(e)
        session.close
        System.exit(1)
      }
    }
  }

  def processCommand(commandSet: CommandSet, session: Session): Unit = { //to-do control table is exist
    commandSet.command match {
      case "exit" => return System.exit(0)
      case "set" => testTable = new TestTable(session, commandSet.parameters.head)
      case "get" => println(testTable.tableName)
      case "create" => testTable.createTable()
      case "insert" => testTable.insertTable(BigInt(commandSet.parameters.head), new BatchStatement())
      case "size" => println(testTable.getTableSize())
      case "equalize" => testTable.equalize(BigInt(commandSet.parameters.head))
      case _ => println("Invalid argument")
    }
    val input: String = scala.io.StdIn.readLine("test> ")
    val newCommandSet: CommandSet = new Parser(input).parse
    return processCommand(newCommandSet, session)
  }
}

class App {
  def readConnectionInfo(): ConnectionInfo = {
    val config = ConfigFactory.load()

    new ConnectionInfo(
      config.getString("cassandra-keyspace"),
      config.getString("cassandra-address"),
      config.getString("cassandra-username"),
      config.getString("cassandra-password"),
      config.getString("cassandra-port")
    )
  }

}
