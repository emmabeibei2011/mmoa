package com.mediamath.oa

import org.apache.spark.sql.SparkSession


object Solution {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val events = spark.read.csv("file:/Users/dishao/SRC/mediamath-oa/events.csv")

    val imps = spark.read.csv("file:/Users/dishao/SRC/mediamath-oa/impressions.csv")
  }
}
