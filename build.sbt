name := "GenerateDataForTest"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.skife.com.typesafe.config" % "typesafe-config" % "0.3.0"
libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "3.1.0"
libraryDependencies += "org.scalatest" % "scalatest_2.10" % "1.9.1" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "1.9.1" % "test"