package dataGenerator

import enums.Serializer
import kafka.producers.CustomProducer
import org.slf4j.LoggerFactory
import schemaRegistry.SchemaRegistry

class RandomDataProducer(schemaRegistryUrl: String, schemaName: String, numberRecord: Int,
                         hasKey: Boolean, fieldsWithCustomValue: Map[String, List[String]],
                         serializerType: Serializer, topic: String) {

  def produceMessages(producer: CustomProducer, records: Iterator[Any]): Unit = {

    serializerType match {
      case Serializer.STRING =>
        records.foreach {
            case (key, value) => producer.send(topic, key.toString, value.toString)
            case value => producer.send(topic, value.toString)
        }
      case Serializer.AVRO =>
        records.foreach {
          case (key, value) => producer.send(topic, key, value)
          case value => producer.send(topic, value)
        }
      case _ =>
        throw new Exception("Type of serialization not valid")

    }
  }

  def getRecordsToWrite: Iterator[Any] = {
    if (hasKey) {

      val keys = getRandomDataBasedOnSchema( schemaName + "-key")
      val values = getRandomDataBasedOnSchema(schemaName + "-value")
      keys zip values

    } else {

      getRandomDataBasedOnSchema(schemaName + "-value")
    }
  }

  private def getRandomDataBasedOnSchema(schemaName: String): Iterator[Any] = {

    lazy val schema = SchemaRegistry.retrieveSchema(schemaRegistryUrl, schemaName)
    val randomData = new RandomData(schema, numberRecord, fieldsWithCustomValue)

    randomData.iterator
  }
}

object RandomDataProducer{
  private val logger = LoggerFactory.getLogger(RandomDataProducer.getClass)
}
