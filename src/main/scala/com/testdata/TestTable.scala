package com.testdata

import java.util.Date

import com.datastax.driver.core._

import scala.util.Random

/**
  * Created by mgunes on 11.08.2016.
  */
class TestTable(val session: Session) {
  private val tableName: String = "recursive_test"
  private val batchCount: Int = 100 // to do : read from conf file
  private val defaultInsertStatement: String = "INSERT INTO " + tableName +
    " (id, user_name, password, name, surname, address, school, age, point, rank) values " + "(now(), "

  def createTable(): Unit = {
    val query: String = "CREATE TABLE " + tableName + "(" +
      "id timeuuid, " +
      "user_name text, " +
      "password text, " +
      "age int, " +
      "name text, " +
      "surname text, " +
      "point int, " +
      "rank int, " +
      "address text, " +
      "school text," +
      "primary key(id, user_name, age));"

    session.execute(query)
    println("created " + tableName)
  }

  def getTableSize():BigInt = {
    val query = session.execute(new SimpleStatement("select count(*) from " + tableName).setReadTimeoutMillis(180000))
    BigInt(query.iterator().next().getObject("count").toString)
  }

  def insertTable(count: BigInt): Unit = {
    count > 0 match {
      case false => println("Insert Operations completed")
      case _ => {
        session.execute(createAnInsertQuery(defaultInsertStatement))
        return insertTable(count - 1)
      }
    }
  }

  def equalize(count: BigInt): Unit = {
    val size = getTableSize

    ((count - size) > 0) match {
      case true => {
        insertTable(count - size)
        println("Inserted " + (count - size) + " row")
      }
      case false => println("You should delete " + (size - count) + " row")
    }
  }

  def createAnInsertQuery(defaultInsert: String): String = {
    val insert: StringBuilder = new StringBuilder
    insert.append(defaultInsert)

    val randomNumber = scala.util.Random
    val user_name = randomAlpha(10)
    val password = randomAlpha(10)
    val age = randomNumber.nextInt(100)
    val name = randomAlpha(10)
    val surname = randomAlpha(10)
    val rank = randomNumber.nextInt(10)
    val point = randomNumber.nextInt(1000) + 1000
    val address = randomAlpha(10)
    val school = randomAlpha(10)

    insert.append("'" + user_name + "'," + "'" + password + "'," + "'" + name + "'," + "'" + surname + "'," +
      "'" + address + "'," + "'" + school + "'," + age + "," + point + "," + rank + ");")

    insert.toString()
  }

  def randomAlpha(length: Int): String = {
    val chars = ('a' to 'z') ++ ('A' to 'Z')
    randomStringFromCharList(length, chars)
  }

  def randomStringFromCharList(length: Int, chars: Seq[Char]): String = {
    val sb = new StringBuilder
    for (i <- 1 to length) {
      val randomNum = util.Random.nextInt(chars.length)
      sb.append(chars(randomNum))
    }
    sb.toString
  }
}
