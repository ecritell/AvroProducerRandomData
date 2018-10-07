import java.util.concurrent.Future

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}

import scala.collection.JavaConverters._


case class CustomProducer(brokerList: String, schemaRegistry: String,
                          keySerializer: String, valueSerializer: String) {
  val SCHEMA_REGISTRY = "schema.registry.url"

  val producerProps = {
    Map[String, Object](
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerList,
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> keySerializer,
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> valueSerializer,
      ProducerConfig.ACKS_CONFIG -> "all",
      ProducerConfig.RETRIES_CONFIG -> "3",
      ProducerConfig.BATCH_SIZE_CONFIG -> "16384",
      ProducerConfig.LINGER_MS_CONFIG -> "1",
      ProducerConfig.BUFFER_MEMORY_CONFIG -> "33554432",
      SCHEMA_REGISTRY -> schemaRegistry
    ).asJava
  }

  val producer = new KafkaProducer[Any, Any](producerProps)

  def send(topic: String, key: Any, value: Any): Future[RecordMetadata] = {
    producer.send(new ProducerRecord[Any, Any](topic, key, value))
  }

  def send(topic: String, value: Any): Future[RecordMetadata] = {
    producer.send(new ProducerRecord(topic, value))
  }
}

object CustomProducer {
  var brokerList = ""
  var schemaRegistry = ""
  var keySerializer = ""
  var valueSerializer = ""
  lazy val instance = CustomProducer(brokerList, schemaRegistry, keySerializer, valueSerializer)
}