
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils

object Consumer {


  val SLIDE_INTERVAL = Seconds(3)
  val WINDOW_LENGTH = Seconds(3)
  //@todo move to HDFS
  val CHECKPOINT_DIRECTORY = "tmp/checkpoint"
  val OUTPUT_PATH = "tmp/output/sales"

  def main(args: Array[String]) {
    val ssc = StreamingContext.getOrCreate(CHECKPOINT_DIRECTORY, functionToCreateContext _)

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

  def functionToCreateContext(): StreamingContext = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Producer")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint(CHECKPOINT_DIRECTORY)

    val topicSet = Set(Kafka.TOPIC)

    val kafkaParams = Map[String, String]("metadata.broker.list" -> Kafka.BROKER)
    val messages: InputDStream[(String, Array[Byte])] = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](
      ssc, kafkaParams, topicSet)

    val deltaFunction = (key: String, value: Option[Int], state: State[Int]) => {
      val currVal = value.getOrElse(0)
      val prevVal = state.getOption.getOrElse(0)
      var delta = 0
      if (prevVal !=0) {
        delta = 100 * (currVal - prevVal) / prevVal;
      }
      val output = (key, currVal, delta + "%")
      state.update(currVal)
      output
    }

    val results = messages
      .map(serRecord => (serRecord._1, Serializer.des(serRecord._2)))
      .map(record => (record._1, Serializer.getTransaction(record._2)))
      .map(t => (t._1, t._2.value))
      .reduceByKeyAndWindow(_+_, WINDOW_LENGTH)
      .mapWithState(StateSpec.function(deltaFunction))
      .checkpoint(SLIDE_INTERVAL)

    results.print()
    results.saveAsTextFiles(OUTPUT_PATH)

    ssc
  }
}