package com.qualys.cleansing.cleansing

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer
import scala.util.Try

object SparkFileCleansing {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    FileUtils.deleteDirectory(new File("src/main/resources/quar"))

    val sparkSession = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    val df = sparkSession.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", value = false)
      .option("badRecordsPath", "src/main/resources/quar.txt")
      .csv("src/main/resources/file/emp.txt")
      .cache()


    df.printSchema()

    val column = df.columns
    //get the schema from the 1st row
    val fieldType: ListBuffer[StructField] = new ListBuffer[StructField]
    val fieldTypeV2: Array[String] = new Array[String](5)
    val firstRow: Seq[Any] = df.first().toSeq
    for (i <- column.indices) {
      checkString(firstRow(i).toString) match {
        case "int" =>
          fieldTypeV2(i) = "int"
          fieldType += StructField(column(i).trim, IntegerType, nullable = false)
        case "double" =>
          fieldTypeV2(i) = "double"
          fieldType += StructField(column(i).trim, DoubleType, nullable = false)
        case "string" =>
          fieldTypeV2(i) = "string"
          fieldType += StructField(column(i).trim, StringType, nullable = false)
      }
    }

    val schema: StructType = StructType(StructField("Error", StringType, nullable = false) +: fieldType.toList)
    val errorRows: List[(String, Row)] = List()
    val cleanedRdd: RDD[Row] = df.rdd.map(row => {
      var newRow = row
      val listRows: ListBuffer[Any] = new ListBuffer[Any]
      try {
        if (row.anyNull) {
          newRow = Row.fromSeq("Less number of rows" +: row.toSeq)
        }
        else {
          for (i <- column.indices) {
            val data = row.get(i)
            val cleanData = data.toString.trim.replaceAll("\"", "").replaceAll("\t", "")
            fieldTypeV2(i) match {
              case "int" => listRows += {
                if (cleanData.isEmpty)
                  0
                else
                  cleanData.toInt
              }
              case "double" => listRows += {
                if (cleanData.isEmpty)
                  0.00
                else
                  cleanData.toDouble
              }
              case "string" => listRows += {
                if (cleanData.isEmpty)
                  null
                else
                  cleanData
              }
            }
          }
        }
      } catch {
        case _: Throwable => newRow = Row.fromSeq("Error in Casting" +: row.toSeq)
      }

      if (listRows.length == column.length) {
        Row.fromSeq("No error" +: listRows)
      } else {
        newRow
      }
    })
    val validRdd = cleanedRdd.filter(_.getString(0).equals("No error"))
    val errorRdd = cleanedRdd.filter(!_.getString(0).equals("No error"))
    val validDF = sparkSession.createDataFrame(validRdd, schema).drop("Error").sort("name")
    validDF.coalesce(1).write
      .option("header", value = true)
      .mode(SaveMode.Overwrite)
      .csv("src/main/resources/output")
    errorRdd.coalesce(1)
      .saveAsTextFile("src/main/resources/quar")
  }

  def checkString(aString: String): String = {
    var ret: String = ""
    if (isInt(aString)) ret = "int"
    else if (isLong(aString)) ret = "long"
    else if (isDouble(aString)) ret = "double"
    else ret = "string"
    ret
  }

  def isShort(aString: String): Boolean = Try(aString.toLong).isSuccess

  def isInt(aString: String): Boolean = Try(aString.toInt).isSuccess

  def isLong(aString: String): Boolean = Try(aString.toLong).isSuccess

  def isDouble(aString: String): Boolean = Try(aString.toDouble).isSuccess

  def isFloat(aString: String): Boolean = Try(aString.toFloat).isSuccess
}