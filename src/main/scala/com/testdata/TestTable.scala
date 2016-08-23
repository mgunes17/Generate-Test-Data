package com.testdata

import com.datastax.driver.core._

/**
  * Created by mgunes on 11.08.2016.
  */
class TestTable(val session: Session, val tableName: String = "default_table_name") {
  private val batchStatement: BatchStatement = new BatchStatement()

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

  def insertTable(count: BigInt, batchStatement: BatchStatement): Unit = {
    count > 0 match {
      case false => println("Insert Operations completed")
      case _ => {
        batchStatement.add(createAnInsertQuery())
        if(batchStatement.size() == 100) {
          session.execute(batchStatement)
          println("100")
          return insertTable(count - 1, batchStatement.clear())
        }

        return insertTable(count - 1, batchStatement)
      }
    }
  }

  def equalize(count: BigInt): Unit = { //to do : delete or truncate-insert . performance
    val size = getTableSize

    ((count - size) > 0) match {
      case true => {
        insertTable(count - size, new BatchStatement())
        println("Inserted " + (count - size) + " row")
      }
      case false => {
        reInsert(count - size)
        println("Deleted " + (size - count) + " row")
      }
    }
  }

  def reInsert(count:BigInt): Unit = {
    session.execute("truncate table " + tableName)
    insertTable(count, new BatchStatement())
  }

  def createAnInsertQuery(): BoundStatement = {
    val defaultInsertStatement: PreparedStatement = session.prepare(
      "INSERT INTO " + tableName +
        " (id, user_name, password, name, surname, address, school, age, point, rank) values " +
        "(now(), ?, ?, ?, ?, ?, ?, ?, ?, ?);")

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

    val boundStatement: BoundStatement = defaultInsertStatement.bind(user_name, password, name, surname, address, school,
      new Integer(age), new Integer(point), new Integer(rank))
    boundStatement
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
