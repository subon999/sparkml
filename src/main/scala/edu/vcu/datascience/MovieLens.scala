package edu.vcu.datascience

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object MovieLens {

  def main(args: Array[String]): Unit = {

    val log = Logger.getLogger("org")

    log.setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("MovieLens")
      .master("local[*]")
      .getOrCreate()
    println("___Spark conf created___")

    import spark.implicits._

    try
      {
        val df = spark.read.option("header",true)
          .csv("/Volumes/DATA/dev/data/ml-25m/ratings.csv")

        println("==========count: " + df.cache.count() + " =================")

        df.show(false)

        println("============================================================")

        df.printSchema()

        val dfnrch=df.groupBy($"movieId").
          agg(count("rating") as "Counts").
          sort($"Counts".desc)

        dfnrch.show(false)

      }

  }
}