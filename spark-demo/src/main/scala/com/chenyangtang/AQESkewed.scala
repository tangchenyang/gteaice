package com.chenyangtang

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object AQESkewed {
  def main(args: Array[String]): Unit = {
    val myconf = new SparkConf()
      .setAppName("aqe")
      .setMaster("local[*]")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.adaptive.coalescePartitions.enabled", "false")
      .set("spark.sql.adaptive.skewJoin.enabled", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.join.preferSortMergeJoin", "false")
      .set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "-1")
      .set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "1")
      .set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "500")

    val spark = SparkSession.builder().config(myconf).getOrCreate()

    val moviesdf = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
      .load("file:///Users/weizhe.huang/Downloads/ml-latest-small/result/movie-skewed.csv")
      .repartition(10)
//      .repartition(col("movieId"))

    val ratingsdf = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
      .load("file:///Users/weizhe.huang/Downloads/ml-latest-small//ratings.csv")
      .repartition(10)
//      .repartition(col("movieId"))

//    moviesdf.groupBy("movieId").count().count()

    moviesdf.printSchema()
    ratingsdf.printSchema()

//    moviesdf.join(ratingsdf, Seq("movieId")).count()

    Thread.sleep(100000)
  }
}
