package com.stratio.spark.structured.streaming.tests

import java.util.concurrent.TimeUnit

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}


object KafkaMain extends App with Logging {

  /** Creating context **/

  val sparkConf = new SparkConf()
    .setAppName("structured-streaming-basic-tests")
    .setMaster("local[*]")
  val sparkSession = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  import sparkSession.implicits._




}

