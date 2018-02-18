package com.stratio.spark.structured.streaming.tests

import java.util.concurrent.TimeUnit

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.StructType


object UnStructuredFileSourceMain extends App with Logging {

  /** Creating context **/

  val sparkConf = new SparkConf()
    .setAppName("structured-streaming-csv-file-tests")
    .setMaster("local[*]")
  val sparkSession = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  /** Initializing source streams **/

  val userSchema = new StructType().add("name", "string").add("age", "integer")

  val csvDF = sparkSession
    .readStream
    .option("sep", ",")
    .schema(userSchema) // Specify schema of the csv files in unstructured sources
    .csv("/tmp/csv")

  val filtered = csvDF.select("name", "age").where("age > 18")

  // select age, count(*) as total from filtered group by age
  val aggregated = csvDF.groupBy("age").agg("*" -> "count")

  /** Creating query to execute **/

  val filteredQuery = filtered.writeStream
    .outputMode(OutputMode.Append())
    //.trigger(Trigger.Once())
    .format("console")
    .queryName("filteredQuery")

  val aggregatedQuery = aggregated.writeStream
    .outputMode(OutputMode.Complete())
    //.trigger(Trigger.Once())
    //.outputMode(OutputMode.Update())
    .format("console")
    .queryName("aggregatedQuery")

  /** Start queries **/

  val filteredExecution = filteredQuery.start()
  val aggregatedExecution = aggregatedQuery.start()

  /** Manage execution **/

  filteredExecution.awaitTermination()
  aggregatedExecution.awaitTermination()

}

