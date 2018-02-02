package com.mediamath.oa

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

import scala.collection.mutable



object Solution {

  // This func transform events to key value pair.
  def eventsToKv(line: String) = {
    val L = line.split(",")
    ((L(2), L(3)), (L(0), L(4)))    // KV pair: (adId, userId), (ts, type)
  }


  // This func transform impressions to key value pair.
  def impsToKv(line: String) = {
    val L = line.split(",")
    ((L(1), L(3)), (L(0), "impression"))  // KV pair: (adId, userId), (ts, "impression")
  }


  /**
    * This function deduplicates and filters attributed events for each partitions
    * @param partitions
    * @return
    */
  def filterAttributedEvent(partitions: Iterator[((String, String), List[(String, String)])]) = {
    // A hashmap for saving the ts from last (adId, userId, eventType)
    val mp = mutable.HashMap[(String, String, String), Int]()

    val output = mutable.ArrayBuffer[Row]()
    var isAttributed = false

    for (p <- partitions) {
      for (value <- p._2) {
        val key = (p._1._1, p._1._2, value._2)   // adId, userId, eventType
        val ts = value._1.toInt
        val eventType = value._2

        // Update flag
        if (eventType == "impression" && ! isAttributed) isAttributed = true

        // Append attributed event and change flag
        if (eventType != "impression" && isAttributed ) {
          if (! mp.contains(key) || ts - mp(key) >= 60) {
            output += (Row(p._1._1, p._1._2, value._1, value._2))
            isAttributed = false
          }
        }

        if (eventType != "impression") mp(key) = ts   // Update map with timestamp
      }
    }

    output.toIterator // Row(adId, userId, ts, eventType)
  }


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordCount")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    // 1. Load Input Files
    val events = sc.textFile("./events.csv")
    val imps = sc.textFile("./impressions.csv")

    // 2. Pre-process: Deduplicate and Filter Attributed Events
    val eventsKv = events.map(eventsToKv)
    val impsKv = imps.map(impsToKv)
    val preDedup = eventsKv.union(impsKv)

    val afterDedup: RDD[Row] = preDedup.groupByKey(4)
      .mapValues(x => x.toList.sortBy(_._1))
      .mapPartitions(filterAttributedEvent)


    val schema = new StructType()
      .add(StructField("adId", StringType, false))
      .add(StructField("userId", StringType, false))
      .add(StructField("timestamp", StringType, false))
      .add(StructField("eventType", StringType, false))

    val attributedEvents = spark.createDataFrame(afterDedup, schema)
    spark.createDataFrame(afterDedup, schema).createOrReplaceTempView("attributedEvents")

    // 3. Output
    // 3.1 count_of_events
    spark.sql("SELECT adId, eventType, count(*) FROM attributedEvents as ae group by adId, eventType")
      .write.format("com.databricks.spark.csv")
      .save("./output/count_of_events.csv")

    // 3.2 count_of_unique_users
    spark.sql("SELECT adId, eventType, count(distinct userId) from ae group by adId, eventType")
      .write.format("com.databricks.spark.csv")
      .save("./output/count_of_unique_users.csv")

  }

}