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
      .config("spark.sql.shuffle.partitions",10)
      .config("park.default.parallelism",10)
      .config("spark.sql.autoBroadcastJoinThreshold", -1)
      .getOrCreate()



    println("___Spark conf created___")

    //--conf spark.sql.shuffle.partitions=500 --conf spark.default.parallelism=500

    import spark.implicits._

    try
      {
        val dfRateings = spark.read.option("header",true)
          .csv("/home/subon999/Documents/development/data/ml-25m/ratings.csv").repartition(6,$"movieId").sortWithinPartitions()

        val dfMovies = spark.read.option("header",true)
          .csv("/home/subon999/Documents/development/data/ml-25m/movies.csv").repartition(6,$"movieId").sortWithinPartitions()

        println("==========partitions: "+ dfRateings.rdd.getNumPartitions +" =================")

        println("==========partitions: "+ dfMovies.rdd.partitions.size +" =================")

        println("==========count: " + dfRateings.cache.count() + " =================")

        println("==========count: " + dfMovies.cache.count() + " =================")
  
  
        //spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

        //df.show(false)

        println("============================================================")

        dfRateings.printSchema()

        val dfTopR=dfRateings.groupBy($"movieId").
          agg(count("rating") as "Counts").
          sort($"Counts".desc)

        val dfTpRN=dfTopR
          .as("r")
            .join(
              dfMovies.as("m"),
              ($"r.movieId"===$"m.movieId"),
              "leftouter"
            ).select($"title"
            ,$"Counts".alias("TimesRated")
            ).sort($"TimesRated".desc)

        dfTpRN.explain()

        dfTopR.show(false)

        dfTpRN.show(false)
        
      }

  }
}
