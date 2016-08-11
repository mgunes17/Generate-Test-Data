package com.testdata

import java.util.Date

import com.datastax.driver.core.Session

/**
  * Created by mgunes on 11.08.2016.
  */
class TestTable(session_1: Session) {
  private val session: Session = session_1
  private val tableName = "usertest"

  def createTable(): Unit = {
    val query: String = "CREATE TABLE " + tableName + "(" +
      "id timeuuid, " +
      "user_name text, " +
      "password text, " +
      "age int, " +
      "primary key(id, user_name, age));"

    session.execute(query)
    println("created " + tableName)
  }

  def insertTable(count: Int): Unit = {
    val insertCount: Int = count
    //var insertArray: List[String] = List()
    val defaultInsert = "INSERT INTO " + tableName + " (id, user_name, password, age) values (now(), 'username', 'password',"
    var insert: String = ""
    val random = scala.util.Random
    //var userName: String = "username"
    //var password: String = "password"
    var age: Int = 0

    for(i<-0 until insertCount) {
      age = random.nextInt(100)
      insert += defaultInsert + age + ");"
      session.execute(insert)
      insert = ""
    }


  }

}
