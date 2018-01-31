package com.mediamath.oa

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders


object Solution {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val eventsSchema = Encoders.product[Event].schema
    val events = spark.read.schema(eventsSchema).csv("file:/Users/dishao/SRC/mediamath-oa/events.csv")

    val impsSchema = Encoders.product[Impression].schema
    val imps = spark.read.schema(impsSchema).csv("file:/Users/dishao/SRC/mediamath-oa/impressions.csv")
  }
}

case class Event(ts: Int, eventId: String, adId: String, userId: String, eventType: String)

case class Impression(ts: Int, adId: String, creativeId: String, userId: String)