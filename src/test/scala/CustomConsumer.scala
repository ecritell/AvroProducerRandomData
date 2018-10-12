import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}


case class CustomConsumer(brokerList: String, schemaRegistryUrl: String,
                          keyDeserializer: String, valueDeserializer: String) {

  val SCHEMA_REGISTRY = "schema.registry.url"
  val AVRO_READER = "specific.avro.reader"

  val props = new Properties()
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group-test")
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer)
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer)
  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
  props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "16384")

  props.put(SCHEMA_REGISTRY, schemaRegistryUrl)
  //props.put(AVRO_READER, "true")

  val consumer = new KafkaConsumer[Any, Any](props)
}
