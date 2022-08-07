name := "SparkTutorial"

version := "0.1"

scalaVersion := "2.13.8"

val postgresVersion = "42.3.6"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.0"
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.29"
//libraryDependencies += "org.apache.livy" % "livy-client-http" % "0.7.1-incubating"
libraryDependencies += "org.postgresql" % "postgresql" % postgresVersion