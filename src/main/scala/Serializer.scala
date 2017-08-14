package util

import java.io.ByteArrayOutputStream

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io._
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter}

import scala.io.Source

object Serializer {

  val schemaSource = Source.fromFile("src/main/resources/transactions.avsc")
  val schema = new Schema.Parser().parse(schemaSource.mkString)
  val writer = new SpecificDatumWriter[GenericRecord](schema)
  val reader = new SpecificDatumReader[GenericRecord](schema)


  def main(args: Array[String]): Unit = {

    val record = new GenericData.Record(schema)
    record.put("distributor", "dist_c")
    record.put("pos", "pos_3")
    record.put("value", 1)

    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(out, null)
    writer.write(record, encoder)
    encoder.flush()
    out.close()

    //from 53 bytes as json to 14 bytes as avro
    val message : Array[Byte] = out.toByteArray()

    // Deserialize and create generic record
    val reader: DatumReader[GenericRecord] = new SpecificDatumReader[GenericRecord](schema)
    val decoder: Decoder = DecoderFactory.get().binaryDecoder(message, null)
    val readRecord: GenericRecord = reader.read(null, decoder)
  }

  def getTransaction(record : GenericRecord): Transaction ={
    Transaction(record.get("distributor").toString,
      record.get("pos").toString,
      record.get("value").toString.toInt)
  }

  def getRecord(transaction : Transaction): GenericData.Record ={
    val record: GenericData.Record = new GenericData.Record(schema)
    record.put("distributor", transaction.distributor)
    record.put("pos", transaction.pos)
    record.put("value", transaction.value)
    record
  }

  def serialize(record : GenericData.Record ): Array[Byte] ={
    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(out, null)
    writer.write(record, encoder)
    encoder.flush()
    out.close()
    out.toByteArray()
  }

  def des(message : Array[Byte]): GenericRecord ={
    val decoder = DecoderFactory.get().binaryDecoder(message, null)
    reader.read(null, decoder)
  }
}
