import java.util
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.streaming.Seconds

import scala.io.Source

object Producer {

  val INPUT_PATH = "src/main/resources/transactions.csv"

  def main(args: Array[String]) {

    val props = new util.HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BROKER)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    val producer = new KafkaProducer[String, Array[Byte]](props)

    val file = Source.fromFile(INPUT_PATH)
    for (line <- file.getLines()) {
      val transaction = Parser.getTransaction(line)
      val record = Serializer.getRecord(transaction)
      val serializedRecord: Array[Byte] = Serializer.serialize(record)
      val message = new ProducerRecord[String, Array[Byte]](KafkaConfig.TOPIC, transaction.distributor, serializedRecord)
      producer.send(message)
      val seconds = scala.util.Random.nextInt(5) + 1
      val sleep = Seconds(seconds).milliseconds
      println(transaction, sleep)
      Thread.sleep(sleep)
    }
  }
}

