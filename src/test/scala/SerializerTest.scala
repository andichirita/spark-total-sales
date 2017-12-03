import Serializer._
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class SerializerTest extends FunSuite with BeforeAndAfterEach {
  val record = new GenericData.Record(schema)

  override def beforeEach() {
    record.put("distributor", "dist_c")
    record.put("pos", "pos_3")
    record.put("value", 1)
  }

  test("test Serialize And Deserialize") {
    //from 53 bytes as json to 14 bytes as avro
    val serializedRecord: Array[Byte] = serialize(record)
    val deserializedRecord: GenericRecord = deserialize(serializedRecord)
    assert(deserializedRecord == record)
  }

  test("testGetTransaction") {
    val returnedTransaction = getTransaction(record)
    val expectedTransaction = Transaction("dist_c", "pos_3", 1)
    assert(returnedTransaction == expectedTransaction)
  }

  test("testGetRecord") {
    val transaction = Transaction("dist_c", "pos_3", 1)
    val returnedRecord = getRecord(transaction)
    assert(returnedRecord == record)
  }
}