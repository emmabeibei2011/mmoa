package com.mediamath.oa

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders

import scala.collection.mutable



object Solution {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordCount")
    val sc = new SparkContext(conf)
//    val spark = SparkSession
//      .builder()
//      .appName("Spark SQL basic example")
//      .config("spark.some.config.option", "some-value")
//      .getOrCreate()

    //val eventsSchema = Encoders.product[Event].schema
    //val events = spark.read.schema(eventsSchema).csv("file:/Users/dishao/SRC/mediamath-oa/events.csv")

    //val impsSchema = Encoders.product[Impression].schema
    //val imps = spark.read.schema(impsSchema).csv("file:/Users/dishao/SRC/mediamath-oa/impressions.csv")

    val textFile = sc.textFile("file:/Users/dishao/SRC/mediamath-oa/events.csv")

    // RDD after deduplicate.
    // Schema: (userId, adId, eventType ), List[timestamp, event-id]
    val dedup = textFile.map(mapFunc)
      .groupByKey(4)
      .mapValues(x => x.toList.sortBy(_(0)))
      .mapPartitions(mapPartitionFunc)

  }


  // Map Line into Key Value Pair
  def mapFunc(line: String) = {
    val l: Array[String] = line.split(",")
    ((l(3), l(2), l(4)), List(l(0), l(1)))
  }

  // Deduplication if timestamp now is less than 60s of the last time stamp
  def mapPartitionFunc(partitions: Iterator[((String, String, String), List[List[String]])] )
    : Iterator[((String, String, String), List[String])] = {
    val mp = mutable.HashMap[(String, String, String), Int]()
    val output = mutable.ArrayBuffer[((String, String, String), List[String])]()

    for (p <- partitions) {
      val key: (String, String, String) = p._1
      val values: Seq[List[String]] = p._2

      for (value <- values) {
        val ts = value(0).toInt
        if (! mp.contains(key)) {
          mp += (key -> ts)
        } else {
          if (mp(key) - ts < 60) {
            mp(key) = ts
            output += ((key, value))
          }
        }
      }
    }
    output.toIterator
  }

}

case class Event(ts: Int, eventId: String, adId: String, userId: String, eventType: String)

case class Impression(ts: Int, adId: String, creativeId: String, userId: String)


/*

val textFile = sc.textFile("file:/Users/dishao/SRC/mediamath-oa/events.csv")

events.orderBy("userId", "adId", "eventType", "ts")

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


val wspec = Window.partitionBy("userId", "adId", "eventType").orderBy("ts")

val e2 = events.withColumn("prevTs", lag(events("ts"), 1).over(wspec)).show()

e2.collect.foreach(println)

val dedup = e2.filter("prevTs is null or ts - prevTs >= 60")

//join impression table on


 */
/*
val rddevents = events.rdd

rddevents.foreach(s => println(s.getAs("userId")))

val counts = textFile.map(line => {
 val l = line.split(",")
((l(1), l(2)), 1)
})

val counts = textFile.map(line => {
val l = line.split(","); ((l(3), l(2), l(4)), line) })

 */
/*

Partition by this

2,7fe40811-7d3b-42b9-83ff-58eee06859d9,purchase

For each partition

filter events if


 */
/*

counts.foreachPartition(p => p.foreach(line => println(line._1._3 )  ))
*/


