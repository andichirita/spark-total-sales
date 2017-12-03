object Parser {

  def getTransaction(line: String): Transaction = {
    val fields = line.split(",")
    Transaction(fields(0), fields(1), fields(2).toInt)
  }
}
