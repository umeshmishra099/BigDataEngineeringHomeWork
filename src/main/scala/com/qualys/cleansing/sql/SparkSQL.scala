package com.qualys.cleansing.sql

import org.apache.spark.sql.SparkSession

import scala.io.Source

object SparkSQL {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\winutils")
    lazy val spark = SparkSession
      .builder()
      .appName("Spark SQL")
      .master("local")
      .config("spark.some.config.option", "some-value")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    /*To insert data in table*/
    val statementStream = getClass.getResourceAsStream("/sql/statements.txt")
    val statements = Source.fromInputStream(statementStream).getLines
    statements.foreach(statement => spark.sqlContext.sql(statement))


    /*Query to execute*/
    val queryStream = getClass.getResourceAsStream("/sql/query.txt")
    val queries = Source.fromInputStream(queryStream).getLines
    queries.foreach(query => {
      val finder = query.split(":")
      if (finder(0) == "Description") {
        println(finder(1))
      } else if (finder(0) == "Query") {
        spark.sqlContext.sql(finder(1)).show
      }
    })

  }
}
