package com.chenyangtang

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col


object SparkAQE {

  def main(args: Array[String]): Unit = {
    val myconf = new SparkConf()
      .setAppName("aqe")
      .setMaster("local[*]")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .set("spark.sql.adaptive.skewJoin.enabled", "true")

    val spark = SparkSession.builder().config(myconf)
      .config("spark.driver.host","127.0.0.1")
      .getOrCreate()

    val moviesdf = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("file:///Users/twadmin/Downloads/ml-latest-small/movies.csv")
      .repartition(col("movieId"))

    moviesdf.groupBy("movieId").count().show()

  }

}
