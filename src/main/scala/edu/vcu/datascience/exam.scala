package edu.vcu.datascience

import scala.collection.mutable


object exam {
	def main(args: Array[String]) {
		var ok = true
		//val list = mutable.MutableList[Any]()
		var a=List.empty[String]
		while (ok) {
			val ln = readLine()
			ok = (ln != null )
			//var b=(scala.util.Try(ln.toDouble).isSuccess == false)
			if (ok) println(ln)
			a :+= ln
		}
		println(a)
		val c=a.filter(_ != null).map(_.split(",").toList)
		println(c)
		c.sortBy(e => e(1))
	}
}
