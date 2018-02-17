package com.stratio.spark.structured.streaming.tests

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType


object StructuredFileSourceMain extends App with Logging {

  /** Creating context **/

  val sparkConf = new SparkConf()
    .setAppName("structured-streaming-json-file-tests")
    .setMaster("local[*]")
  //sparkConf.set("spark.sql.streaming.schemaInference", "true")
  val sparkSession = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  /** Initializing source streams **/

  val userSchema = new StructType().add("name", "string").add("age", "integer")

  val jsonDF = sparkSession
    .readStream
    .schema(userSchema) // Specify schema of the json
    .json("/tmp/json")

  val filtered = jsonDF.select("name", "age").where("age > 18")
  val aggregated = jsonDF.groupBy("age").agg("*" -> "count")

  /** Creating query to execute **/

  val filteredQuery = filtered.writeStream
    .outputMode(OutputMode.Append())
    .format("console")
    .queryName("filteredQuery")

  val aggregatedQuery = aggregated.writeStream
    .outputMode(OutputMode.Complete())
    .format("console")
    .queryName("aggregatedQuery")

  /** Start queries **/

  val filteredExecution = filteredQuery.start()
  val aggregatedExecution = aggregatedQuery.start()

  /** Manage execution **/

  filteredExecution.awaitTermination()
  aggregatedExecution.awaitTermination()

}

