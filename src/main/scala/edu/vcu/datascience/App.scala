package edu.vcu.datascience

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.io._


/**
 * Hello world!
 *
 */
object App extends Serializable {

  def main(args: Array[String]): Unit =  {
    println("Hello World!")
    //val log = LogManager.getLogger(getClass.getName)
    val log = Logger.getLogger("org")

    log.setLevel(Level.ERROR)

    println("---------------------------------------- START OF SCALA EXECUTION ----------------------------------------")
    println("____________________ Project Security Payment LR XFM _________________")
    println("---------------Create the load ready File from the Source File---------------------------------------------")
    println("\n")
    println("___Spark Conference___")

    val spark = SparkSession.builder()
      .appName("Sales")
      .master("local[*]")
      .getOrCreate()
    println("___Spark conf created___")

    import spark.implicits._

    try {
      val df = spark.read.option("header",true)
        .csv("/Volumes/DATA/dev/sparkml/data/1500000SalesRecords.csv")

      println(df.cache().count())



      df.sort('Country,$"Sales Channel".desc,$"Item Type".desc).limit(10).show(false)

      val df_nrch = df.groupBy($"Item Type").agg(avg("Total Profit") as "avg")

      df_nrch.show(false)

      val df_nrch_1 = df.groupBy($"Item Type").
        agg(count("Order ID") as "count", sum("Total Revenue" ).
          cast("decimal(28,2)" ).alias("sum"))

      df_nrch_1.show(false)

      println(df_nrch.queryExecution.logical.numberedTreeString)

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
