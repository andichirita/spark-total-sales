import Parser._
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class ParserTest extends FunSuite {

  test("testGetTransaction") {
    val transaction1 = getTransaction("dist_a,pos_1,5")
    assert(transaction1.distributor === "dist_a")
    assert(transaction1.pos === "pos_1")
    assert(transaction1.value === 5)

    val transaction2 = getTransaction("dist_a,pos_2,2")
    assert(transaction2.distributor === "dist_a")
    assert(transaction2.pos === "pos_2")
    assert(transaction2.value === 2)
  }

  test("testGetTransactionWithError") {
    assertThrows[ArrayIndexOutOfBoundsException] {
      getTransaction(System.lineSeparator())
    }
  }
}
