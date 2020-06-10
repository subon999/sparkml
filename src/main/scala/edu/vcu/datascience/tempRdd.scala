package edu.vcu.datascience

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import scala.math.min

object tempRdd {

  def parseLines(line:String) = {
    val col=line.split(",")
    val stationID = col(0)
    val entryType = col(2)
    val temperature = col(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationID, entryType, temperature)
  }

  def main(args: Array[String]): Unit = {

    val log = Logger.getLogger("org")

    log.setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("TempRdd")
      .master("local[*]")
      .getOrCreate()
    println("___Spark conf created___")

    val sc=spark.sparkContext

    val tRdd=sc.textFile("/Volumes/DATA/dev/sparkml/data/1800.csv")

    val tRddn=tRdd.map(parseLines)

    println(tRddn.count())

    //tRdd.top(10).foreach(println)

    //tRddn.top(10).foreach(println)

    val rdd_TMin=tRddn.filter(x => x._2=="TMIN")

    //rdd_TMin.top(10).foreach(println)

    val stationTemps = rdd_TMin.map(x => (x._1, x._3.toFloat))

    val stationTempsC = rdd_TMin.map(x => (x._1, 1))

    val minTempsByStation = stationTemps.reduceByKey( (x,y) => min(x,y))

    val co = stationTempsC.reduceByKey( (x,y) => x + y).map(x => (x._2, x._1)).sortByKey()

    minTempsByStation.foreach(println)

    co.foreach(println)


  }

}
