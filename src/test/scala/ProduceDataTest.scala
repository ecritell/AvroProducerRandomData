import java.util

import ProduceData.CHOOSE_SERIALIZER
import dataGenerator.RandomDataProducer
import enums.Serializer
import kafka.producers.CustomProducer
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords}
import org.scalatest.BeforeAndAfterAll

import sys.process._
import scala.collection.JavaConverters._

class ProduceDataTest extends UnitSpec with BeforeAndAfterAll {

  val CHOOSE_DESERIALIZER: Map[Serializer, String] = Map[Serializer, String](
    Serializer.AVRO -> "io.confluent.kafka.serializers.KafkaAvroDeserializer",
    Serializer.STRING -> "org.apache.kafka.common.serialization.StringDeserializer"
  )

  val brokers: String = "http://" + System.getProperty("broker:9093")
  val schemaRegistryUrl: String = "http://" + System.getProperty("schema_registry:8081")
  val schemaName: String = "gb.portability.activity"

  val fieldsWithCustomValue: Map[String, List[String]] = Map("providerTerritory" -> List("gb", "ie", "at", "es", "it"),
    "provider" -> List("nowtv", "sky"),
    "proposition" -> List("sky", "skyplus", "skyq"))

  val consumerPollTimeout = 5000

  val schemaValue: String = scala.io.Source.fromURL(getClass.getResource("/AvroSchemaValue")).getLines.mkString
  val schemaKey: String = scala.io.Source.fromURL(getClass.getResource("/AvroSchemaKey")).getLines.mkString

  val curlSchemaValue = Seq("curl", "-X", "POST", "-H", "Content-Type: application/vnd.schemaregistry.v1+json", "--data",
    s"$schemaValue", schemaRegistryUrl + "/subjects/" + schemaName + "-value/versions")

  val curlSchemaKey = Seq("curl", "-X", "POST", "-H", "Content-Type: application/vnd.schemaregistry.v1+json", "--data",
    s"$schemaKey", schemaRegistryUrl + "/subjects/" + schemaName + "-key/versions")

  Thread.sleep(60000)

  curlSchemaValue.!
  curlSchemaKey.!


  "Produce avro data type with key" should "write record in kafka topic" in {

    val topicName = "gb.portability.activity.avro"
    val numberRecord = 1
    val hasKey = "true".toBoolean
    val serializerType = Serializer.AVRO

    val keyDeserializer = CHOOSE_DESERIALIZER(serializerType)
    val valueDeserializer = CHOOSE_DESERIALIZER(serializerType)


    val producer = CustomProducer(brokers, schemaRegistryUrl, CHOOSE_SERIALIZER(serializerType), CHOOSE_SERIALIZER(serializerType))

    val randomDataProducer = new RandomDataProducer(schemaRegistryUrl, schemaName,
      numberRecord, hasKey, fieldsWithCustomValue, serializerType, topicName)

    val records = randomDataProducer.getRecordsToWrite

    assert (records.size == numberRecord)

    randomDataProducer.produceMessages(producer, records)
    producer.producer.close()

    val consumer = CustomConsumer(brokers, schemaRegistryUrl, keyDeserializer, valueDeserializer).consumer

    consumer.subscribe(util.Collections.singletonList(topicName))

    /*
    while (true) {
      val records = consumer.poll(100)

      for (record: ConsumerRecord[Any, Any] <- records.asScala) {
        println(record.value())

      }
    }
    */


    val recordsPolled: ConsumerRecords[Any, Any] = consumer.poll(consumerPollTimeout)

    recordsPolled.iterator().hasNext shouldBe true


    val record = recordsPolled.iterator().next()

    record.key() shouldBe records.toList.head.asInstanceOf[(Any, Any)]._1
    record.value() shouldBe records.toList.head.asInstanceOf[(Any, Any)]._2


    consumer.commitSync()
    consumer.close()

  }


  "Produce avro data type without key" should "write record in kafka topic" in {

    val topicName = "gb.portability.activity.avro"
    val numberRecord = 1
    val hasKey = "false".toBoolean
    val serializerType = enums.Serializer.AVRO

    val keyDeserializer = CHOOSE_DESERIALIZER(serializerType)
    val valueDeserializer = CHOOSE_DESERIALIZER(serializerType)

    val producer = kafka.producers.CustomProducer(brokers, schemaRegistryUrl, CHOOSE_SERIALIZER(serializerType), CHOOSE_SERIALIZER(serializerType))

    val randomDataProducer = new RandomDataProducer(schemaRegistryUrl, schemaName,
      numberRecord, hasKey, fieldsWithCustomValue, serializerType, topicName)

    val records = randomDataProducer.getRecordsToWrite

    assert (records.size == numberRecord)

    randomDataProducer.produceMessages(producer, records)
    producer.producer.close()

    val consumer = CustomConsumer(brokers, schemaRegistryUrl, keyDeserializer, valueDeserializer).consumer

    consumer.subscribe(util.Collections.singletonList(topicName))

    val recordsPolled: ConsumerRecords[Any, Any] = consumer.poll(consumerPollTimeout)

    recordsPolled.iterator().hasNext shouldBe true


    val record = recordsPolled.iterator().next()

    assert(record.key() == null)
    assert(record.value() == records.toList.head)


    consumer.commitSync()
    consumer.close()
  }

  "Produce string data type without key" should "write record in kafka topic" in {

    val topicName = "gb.portability.activity"
    val numberRecord = 1
    val hasKey = "false".toBoolean
    val serializerType = enums.Serializer.STRING

    val keyDeserializer = CHOOSE_DESERIALIZER(serializerType)
    val valueDeserializer = CHOOSE_DESERIALIZER(serializerType)

    val producer = kafka.producers.CustomProducer(brokers, schemaRegistryUrl, CHOOSE_SERIALIZER(serializerType), CHOOSE_SERIALIZER(serializerType))

    val randomDataProducer = new RandomDataProducer(schemaRegistryUrl, schemaName,
      numberRecord, hasKey, fieldsWithCustomValue, serializerType, topicName)

    val records = randomDataProducer.getRecordsToWrite

    assert (records.size == numberRecord)

    randomDataProducer.produceMessages(producer, records)
    producer.producer.close()

    val consumer = CustomConsumer(brokers, schemaRegistryUrl, keyDeserializer, valueDeserializer).consumer

    consumer.subscribe(util.Collections.singletonList(topicName))

    val recordsPolled: ConsumerRecords[Any, Any] = consumer.poll(consumerPollTimeout)

    recordsPolled.iterator().hasNext shouldBe true

    assert(recordsPolled.asScala.head.key() == null)
    assert(recordsPolled.asScala.head.value() == records.toList.head.toString)

    consumer.commitSync()
    consumer.close()
  }

  "Produce string data type with key" should "write record in kafka topic" in {

    val topicName = "gb.portability.activity.avro"
    val numberRecord = 1
    val hasKey = "true".toBoolean
    val serializerType = enums.Serializer.STRING

    val keyDeserializer = CHOOSE_DESERIALIZER(serializerType)
    val valueDeserializer = CHOOSE_DESERIALIZER(serializerType)

    val producer = kafka.producers.CustomProducer(brokers, schemaRegistryUrl, CHOOSE_SERIALIZER(serializerType), CHOOSE_SERIALIZER(serializerType))

    val randomDataProducer = new RandomDataProducer(schemaRegistryUrl, schemaName,
      numberRecord, hasKey, fieldsWithCustomValue, serializerType, topicName)

    val records = randomDataProducer.getRecordsToWrite

    assert (records.size == numberRecord)

    randomDataProducer.produceMessages(producer, records)
    producer.producer.close()

    val consumer = CustomConsumer(brokers, schemaRegistryUrl, keyDeserializer, valueDeserializer).consumer

    consumer.subscribe(util.Collections.singletonList(topicName))

    val recordsPolled: ConsumerRecords[Any, Any] = consumer.poll(consumerPollTimeout)

    recordsPolled.iterator().hasNext shouldBe true

    assert(recordsPolled.asScala.head.key() == records.toList.head.asInstanceOf[(Any, Any)]._1.toString)
    assert(recordsPolled.asScala.head.value() == records.toList.head.asInstanceOf[(Any, Any)]._2.toString)

    consumer.commitSync()
    consumer.close()
  }

}
