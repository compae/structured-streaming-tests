package com.stratio.spark.structured.streaming.tests

import java.util.concurrent.TimeUnit

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType


object AggregationsMain extends App with Logging {

  /** Creating context **/

  val sparkConf = new SparkConf()
    .setAppName("structured-streaming-aggregations-tests")
    .setMaster("local[*]")
  val sparkSession = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  import sparkSession.implicits._


  /** Initializing source streams **/

  // Reading from fixed data
  val fixedData = sparkSession.readStream
    .format("rate")
    .load()


  // Reading from Socket nc -lk 9999
  val lines = sparkSession.readStream
    .format("socket")
    .option("host", "127.0.0.1")
    .option("port", 9999)
    .load()


  /** Creating aggregations to execute **/

  // Adding column to aggregate sqrt value
  val fixedDataWithSqrt = fixedData.withColumn("roundsqrt", round(sqrt("value")))

  // Split the lines into words
  val words = lines.as[String]
    .flatMap(lineString => lineString.split(" "))
    .withColumn("company", lit(Literal("stratio")))
    .withColumn("employees", lit(Literal(300)))

  // Generate running basic counts
  val sqrtCounts = fixedDataWithSqrt
    .groupBy("roundsqrt") //Necessary aggregation: groupBy, cube, rollup
    .count()

  val wordCounts = words
    .groupBy("value") //Necessary aggregation: groupBy, cube, rollup
    .count()

  //The available aggregate methods are avg, max, min, sum, count, stddev.
  val sqrtAggregations = fixedDataWithSqrt.groupBy("roundsqrt")
    .agg(
      "roundsqrt" -> "max",
      "roundsqrt" -> "min",
      "roundsqrt" -> "avg",
      "roundsqrt" -> "sum",
      "*" -> "count"
    )

  val employeeAggregations = words.groupBy("value")
    .agg(
      "employees" -> "max",
      "employees" -> "min",
      "employees" -> "avg",
      "employees" -> "sum",
      "*" -> "count"
    )


  /** Creating queries to execute **/

  // Second query over modified data
  val sqrtCountsQuery = sqrtCounts.writeStream
    .outputMode(OutputMode.Complete())
    .trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
    .format("console")
    .queryName("randomCounts")

  val wordCountsQuery = wordCounts.writeStream
    .outputMode(OutputMode.Complete())
    .format("console")
    .queryName("wordCounts")

  val sqrtAggregationsQuery = sqrtAggregations.writeStream
    .outputMode(OutputMode.Complete())
    //.outputMode(OutputMode.Update()) //Check with watermark
    //.outputMode(OutputMode.Append()) //Not supported in aggregations without watermark!!
    //.option("checkpointLocation", "/tmp/checkpoint")
    .trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
    .format("console")
    .queryName("fixedDataAggregations")

  val employeeAggregationsQuery = employeeAggregations.writeStream
    .outputMode(OutputMode.Complete())
    .format("console")
    .queryName("employeeAggregations")


  /** Start queries **/

  val sqrtCountsExecution = sqrtCountsQuery.start()
  val wordCountsExecution = wordCountsQuery.start()
  //val sqrtAggregationsExecution = sqrtAggregationsQuery.start()
  //val employeeAggregationsExecution = employeeAggregationsQuery.start()


  /** Manage execution **/

  sqrtCountsExecution.awaitTermination()
  wordCountsExecution.awaitTermination()
  //sqrtAggregationsExecution.awaitTermination()
  //employeeAggregationsExecution.awaitTermination()

}

