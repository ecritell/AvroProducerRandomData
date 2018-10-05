import org.apache.avro._
import org.slf4j.LoggerFactory

object RandomDataGeneratorProducer {
  private val logger = LoggerFactory.getLogger(RandomDataGeneratorProducer.getClass)

  def produceMasseges(producer: CustomProducer, serializerType: Serializer, records: Seq[Any], topic: String): Unit = {

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

    producer.producer.flush()
  }

  def getRecordsToWrite(schemaRegistryUrl: String, schemaName: String, numberRecord: Int, hasKey: Boolean): Seq[Any] = {

    if (hasKey) {

      val keys = getRandomDataBasedOnSchema(schemaRegistryUrl, schemaName + "-key", numberRecord).toList
      val values = getRandomDataBasedOnSchema(schemaRegistryUrl, schemaName + "-value", numberRecord).toList
      keys zip values

    } else {

      getRandomDataBasedOnSchema(schemaRegistryUrl, schemaName + "-value", numberRecord).toList
    }

  }

  private def getRandomDataBasedOnSchema(schemaRegistryUrl: String, schemaName: String, numberRecord: Int): Iterator[Any] = {

    try {
      val schema = SchemaRegistry.retrieveSchema(schemaRegistryUrl, schemaName)
      val randomData = new RandomData(schema, numberRecord)

      randomData.iterator
    } catch {
      case e: Exception =>
        val DEFAULTSCHEMA$: Schema = (new Schema.Parser).parse( """ {"type": "string"} """ )
        logger.warn( s"No schema on the schema registry. We're using default schema: ${DEFAULTSCHEMA$.toString}" )

        val randomData = new RandomData(DEFAULTSCHEMA$, numberRecord)
        randomData.iterator
    }
  }
}
