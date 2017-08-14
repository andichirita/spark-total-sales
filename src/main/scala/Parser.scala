package util

import scala.io.Source

object Parser {

  def main(args: Array[String]) {
    val file = Source.fromFile("src/main/resources/transactions.csv")
    for (line <- file.getLines()) {
      val fields = line.split(",")
      fields.foreach(println)
      println(getTransaction(line))
    }
  }

  def getTransaction(line: String): Transaction = {
    val fields = line.split(",")
    Transaction(fields(0), fields(1), fields(2).toInt)
  }
}
