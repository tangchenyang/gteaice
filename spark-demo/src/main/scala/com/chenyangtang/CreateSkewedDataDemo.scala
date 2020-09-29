package com.chenyangtang

import org.apache.spark.SparkConf
import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.apache.spark.sql.functions.col


object CreateSkewedDataDemo {

  def main(args: Array[String]): Unit = {

    val myconf = new SparkConf()
      .setAppName("aqe")
      .setMaster("local[*]")

    myconf.set("spark.default.parallelism", "1")


    val spark = SparkSession.builder().config(myconf).getOrCreate()

    import spark.implicits._

//        val moviesdf = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
//          .load("file:///Users/weizhe.huang/Downloads/ml-latest/movies.csv").as[Movie]
//
//        moviesdf.printSchema()
//
//        moviesdf.map(x => {
//          val movieId = x.movieId
//          if (movieId > 10) {
//            (100000, x.title, x.genres)
//          } else {
//            (movieId, x.title, x.genres)
//          }
//        }).write.format("csv").mode("overwrite").save("file:///Users/weizhe.huang/Downloads/ml-latest/result")

    val ratingsdf = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
      .load("file:///Users/weizhe.huang/Downloads/ml-latest/ratings.csv").as[Rating]

    ratingsdf.printSchema()

    ratingsdf.map(x => {
      val movieId = x.movieId
      if (movieId > 10) {
        (x.userId, 100000, x.rating, x.timestamp)
      } else {
        (x.userId, x.movieId, x.rating, x.timestamp)
      }
    }).write.format("csv").mode("overwrite").save("file:///Users/weizhe.huang/Downloads/ml-latest/result1")


  }

}

case class Movie(movieId: Int, title: String, genres: String)

case class Rating(userId: Int, movieId: Int, rating: Double, timestamp: Int)


