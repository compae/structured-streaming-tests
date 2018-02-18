package com.stratio.spark.structured.streaming.tests

import java.util.concurrent.TimeUnit

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.StructType


object SqlPipelinesMain extends App with Logging {

  /** Creating context **/

  val sparkConf = new SparkConf()
    .setAppName("structured-streaming-batch-tables-tests")
    .setMaster("local[*]")
  val sparkSession = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()


  /** Initializing source streams **/

  val userSchema = new StructType().add("name", "string").add("age", "integer")

  val csvDF = sparkSession
    .readStream
    .schema(userSchema) // Specify schema of the json
    .csv("/tmp/csv")

  val jsonDF = sparkSession.
    read. // <-- batch non-streaming query
    json("/tmp/json/json.json")

  /** Creating queries to execute **/

  // Not available on streaming tables if created before!!
  /*
  jsonDF.createTempView("jsonwrong")
  val jsonwrong = sparkSession.sql("select * from jsonwrong")
  jsonwrong.show()
  */

  val consoleQuery = csvDF.writeStream
    .outputMode(OutputMode.Append())
    .format("console")
    .queryName("consolequery")
    .start()

  val memoryQuery = csvDF.writeStream
    .outputMode(OutputMode.Append())
    .format("memory")
    .queryName("csvquery")
    .start()

  jsonDF.createTempView("jsondata")
  sparkSession.sql("select * from jsondata").show()

  // Executed in the fist stream, change batch mode
  val joinData = sparkSession.sql("select js.name, cd.name, js.age, cd.age from csvquery js join jsondata cd on js.name = cd.name")
  joinData.show()

  // Executed in all the streams, not change to batch
  val joinQuery = csvDF.crossJoin(jsonDF).writeStream
    .outputMode(OutputMode.Append())
    .format("console")
    .queryName("resultquery")
    .start()


  /** Manage execution **/

  //memoryQuery.awaitTermination()
  //consoleQuery.awaitTermination()
  //joinQuery.awaitTermination()

}

