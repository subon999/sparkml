package edu.vcu.datascience

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._



import scala.math.min

object tempDF {

  def main(args: Array[String]): Unit = {

    val log = Logger.getLogger("org")

    log.setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("TempDF")
      .master("local[*]")
      .getOrCreate()
    println("___Spark conf created___")

    import spark.implicits._

    try {
      val df = spark.read.option("header", false)
        .csv("/home/subon999/Documents/development/data/ml-25m/1800.csv")


      val dfNrch=df.filter('_c2==="TMIN").
        withColumn("temperature",('_c3*0.1f * (9.0f / 5.0f) + 32.0f).cast("Decimal(10,2)")).
        select('_c0.alias("stationID")
        ,'temperature
      ).cache()

      //dfNrch.show()

      dfNrch.groupBy($"stationID").agg(count("temperature") as "count").show()

      dfNrch.groupBy($"stationID").min("temperature" ).alias("minTemp").show()



    }catch {
      case ex: Exception => log.debug("___Exception Caught___", ex)
        System.exit(1)
    } finally {
      log.debug("___Profile Deposit PIT XFM Job completed___")
      println("___Profile Deposit PIT XFM Job completed___")
      log.debug("___Ending SparkSession___")
      println("___Ending SparkSession___")
      spark.stop()
    }


  }

}
