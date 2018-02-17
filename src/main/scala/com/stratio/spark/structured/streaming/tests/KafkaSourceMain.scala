package com.stratio.spark.structured.streaming.tests

import java.util.concurrent.TimeUnit

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}


object KafkaSourceMain extends App with Logging {

  /** Creating context **/

  val sparkConf = new SparkConf()
    .setAppName("structured-streaming-basic-tests")
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




  /*
  //SQL compatibility
  //words.createOrReplaceTempView("events")
  //val wordCounts = sparkSession.sql("select count(*) from events")
  */
  /*
  //IN-MEMORY
  val aggregates = wordCounts
    .writeStream
    .queryName("aggregates")    // this query name will be the table name
    .outputMode("complete")
    .format("memory")
    .start()
  sparkSession.sql("select value from aggregates").show()*/


  /*
  // Read all the csv files written atomically in a directory
  val userSchema = new StructType().add("name", "string").add("age", "integer") //spark.sql.streaming.schemaInference=true
  val csvDF = sparkSession
    .readStream
    .option("sep", ",")
    .schema(userSchema)      // Specify schema of the csv files
    .csv("/tmp/csv/")

  val jsonDF = sparkSession
    .readStream
    .schema(userSchema)      // Specify schema of the json files (optional)
    .json("/tmp/json/")


    // Select the devices which have signal more than 10
    case class Employee(name: String, age: Int)

    df.select("device").where("signal > 10")      // using untyped APIs
    ds.filter(_.signal > 10).map(_.device)         // using typed APIs

    // Running count of the number of updates for each device type
    df.groupBy("deviceType").count()                          // using untyped API

    // Running average signal for each device type
    import org.apache.spark.sql.expressions.scalalang.typed
    ds.groupByKey(_.deviceType).agg(typed.avg(_.signal))    // using typed API

    */

}

