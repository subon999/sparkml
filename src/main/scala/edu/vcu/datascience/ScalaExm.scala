package edu.vcu.datascience

import java.io.Serializable

object ScalaExm extends Serializable {

  def main(args: Array[String]): Unit = {

    // Point values for two competing hands
    val handA = 17
    val handB = 19
    // Print the value of the hand with the most points
    if (handA > handB) println(handA)
    else println(handB)

  }
}