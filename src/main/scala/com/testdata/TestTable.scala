package com.testdata

import java.util.Date

import com.datastax.driver.core._

import scala.util.Random

/**
  * Created by mgunes on 11.08.2016.
  */
class TestTable(session_1: Session) {
  private val session: Session = session_1
  private val tableName = "usertest_temp"

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

  def showTableSize(): Unit = {
    val xx = session.execute(new SimpleStatement("select count(*) from " + tableName).setReadTimeoutMillis(180000))
    println(xx.iterator().next().getObject("count"))
  }

  def insertTable(count: Int): Unit = {
    val insertCount: Int = count
    var batchCount: Int = 0
    //var insertArray: List[String] = List()
    val defaultInsert = "INSERT INTO " + tableName + " (id, user_name, password, name, surname, address, school, age, point, rank) values " +
      "(now(), "

    val insert: StringBuilder = new StringBuilder
    insert.append("BEGIN BATCH ")


    val randomNumber = scala.util.Random
    //var userName: String = "username"
    //var password: String = "password"

    for(i<-0 until insertCount) {
      insert.append(defaultInsert)
      var user_name = randomAlpha(10)
      var password = randomAlpha(10)
      var age = randomNumber.nextInt(100)
      var name = randomAlpha(10)
      var surname = randomAlpha(10)
      var rank = randomNumber.nextInt(10)
      var point = randomNumber.nextInt(1000) + 1000
      var address = randomAlpha(10)
      var school = randomAlpha(10)

      insert.append("'"+user_name+"'," + "'"+password+"'," + "'"+name+"'," + "'"+surname+"'," + "'"+address + "',"+"'"+school+"'," + age+","+ point+"," + rank + ");")
      batchCount = batchCount + 1
      //println(insert.toString())

      if(batchCount == 100 ) {
        batchCount = 0
        insert.append(" APPLY BATCH")
        session.execute(insert.toString())
        insert.clear()
        insert.append("BEGIN BATCH ")
        //println("BATCH has finished")
      }
      //println(i)
    }
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
