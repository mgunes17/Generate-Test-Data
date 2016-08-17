package com.testdata

import com.datastax.driver.core._
import com.typesafe.config.ConfigFactory

/**
  * Created by mgunes on 11.08.2016.
  */
object App {
  def main (arguments: Array[String]): Unit = {
    val connectionInfo : ConnectionInfo = new App().readConnectionInfo()

    val builder = Cluster.builder().addContactPoint(connectionInfo.address)
    val cluster = builder.build()
    val session = cluster.connect(connectionInfo.keysapce)

    var testTable: TestTable = new TestTable(session)

    try {
      arguments(0) match {
        case "create" => testTable.createTable()
        case "insert" => testTable.insertTable(arguments(1).toInt)
        case "size" => testTable.showTableSize()
        case _ => println("Invalid argument")
      }
      System.exit(0)
    } catch {
      case e: Exception => {
        println(e)
        System.exit(1)
      }
    }

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
