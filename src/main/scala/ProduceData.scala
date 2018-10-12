import conf.Config
import dataGenerator.RandomDataProducer
import org.json4s._
import org.json4s.jackson.JsonMethods._
import enums.Serializer
import kafka.producers.CustomProducer
import org.slf4j.LoggerFactory
import pureconfig.error.ConfigReaderFailures

import scala.util.Try

object ProduceData {
  private val logger = LoggerFactory.getLogger(ProduceData.getClass)

  val CHOOSE_SERIALIZER: Map[Serializer, String] = Map[Serializer, String](
    Serializer.AVRO -> "io.confluent.kafka.serializers.KafkaAvroSerializer",
    Serializer.STRING -> "org.apache.kafka.common.serialization.StringSerializer"
  )

  def main(args: Array[String]): Unit = {

    implicit val formats = org.json4s.DefaultFormats

    val config: Either[ConfigReaderFailures, Config] = pureconfig.loadConfig[Config]

    if (config.isLeft) {
      logger.error(s"Error during loaded config: ${config.toString}")

      throw new IllegalArgumentException()
    }

    val configLoaded = config.right.get

    var fieldsWithCustomValue: Try[Map[String, List[String]]] = Try(parse(configLoaded.fieldsWithCustomValue)
      .extract[Map[String, List[String]]])

    if(fieldsWithCustomValue.isFailure) {

      throw new IllegalArgumentException("Error during loaded config", fieldsWithCustomValue.failed.get)
    }

    CustomProducer.brokerList = configLoaded.brokersList
    CustomProducer.schemaRegistry = configLoaded.schemaRegistryUrl
    CustomProducer.keySerializer = CHOOSE_SERIALIZER(configLoaded.serializerType)
    CustomProducer.valueSerializer = CHOOSE_SERIALIZER(configLoaded.serializerType)


    val randomDataProducer = new RandomDataProducer(configLoaded.schemaRegistryUrl, configLoaded.schemaName,
      configLoaded.numberRecord, configLoaded.hasKey, fieldsWithCustomValue.get,
      configLoaded.serializerType, configLoaded.topicName)

    logger.info("PRODUCER STARTED")

    val records: Iterator[Any] = randomDataProducer.getRecordsToWrite

    try {

      randomDataProducer.produceMessages(CustomProducer.instance, records)

    }finally {
      CustomProducer.instance.producer.flush()
      CustomProducer.instance.producer.close()
    }

    logger.info("PRODUCER FINISHED")


  }

}
