package com.stratio.spark.structured.streaming.help

import java.util.concurrent.TimeUnit

import akka.event.slf4j.SLF4JLogging
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Encoder, Row, SparkSession}


object WindowWatermarksMain extends App with SLF4JLogging {

  /** Creating context **/

  val sparkConf = new SparkConf()
    .setAppName("structured-streaming-basic-tests")
    .setMaster("local[*]")
  val sparkSession = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  import org.apache.spark.sql.ForeachWriter
  import sparkSession.implicits._

  val writer = new ForeachWriter[Row] {
    override def open(partitionId: Long, version: Long) = true
    override def process(value: Row) = log.info(value.toString())
    override def close(errorOrNull: Throwable) = {}
  }


  /** Initializing source streams **/

  val lines = sparkSession.readStream
    .format("socket")
    .option("host", "127.0.0.1")
    .option("port", 9999)
    .load()

  val schema = StructType(Seq(StructField("value", StringType), StructField("fecha", TimestampType)))

  implicit def rowEncoder(schema: StructType): Encoder[Row] = RowEncoder(schema)

  val words = lines.as[String]
    .flatMap(lineString => lineString.split(" "))
    .toDF()
    .withColumn("fecha", current_timestamp())
    /*.map { row =>
      val oldvalues = row.toSeq
      val newValues = {
        if(row.toString().contains("minute"))
          oldvalues :+ new Timestamp(System.currentTimeMillis() - 216000)
        else if(row.toString().contains("hour"))
          oldvalues :+ new Timestamp(System.currentTimeMillis() - 12960000)
        else if(row.toString().contains("day"))
          oldvalues :+ new Timestamp(System.currentTimeMillis() - 311040000)
        else
          oldvalues :+ new Timestamp(System.currentTimeMillis())
      }

      new GenericRowWithSchema(newValues.toArray, schema).asInstanceOf[Row]
    }(RowEncoder(schema))*/


  /** Creating aggregations to execute **/

  val randomCounts = words
    .withWatermark("fecha", "1 minutes")
    .groupBy(window($"fecha", "1 minutes", "1 minutes") as "groupdate", $"value")
    .agg(count("value") as "count")


  /** Creating queries to execute **/

  // Second query over modified data
  val randomCountsQuery = randomCounts.writeStream
    .outputMode(OutputMode.Update())
    .trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
    .format("console")
    .queryName("randomCountsQuery")

  val writerQuery = randomCounts.writeStream
    .outputMode(OutputMode.Complete())
    .trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
    .foreach(writer)
    .queryName("writerQuery")

  /** Start queries **/

  val randomCountsExecution = randomCountsQuery.start()
  //val writerExecution = writerQuery.start()


  /** Manage execution **/

  randomCountsExecution.awaitTermination()
  //writerExecution.awaitTermination()

}

