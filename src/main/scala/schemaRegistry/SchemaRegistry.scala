package schemaRegistry

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.Schema
import org.slf4j.LoggerFactory

object SchemaRegistry {

  private val logger = LoggerFactory.getLogger(SchemaRegistry.getClass)

  def retrieveSchema(url: String, schemaName: String): Schema = {

    val schemaRegistry = new CachedSchemaRegistryClient(url, 100)

    try {
      val schemaMetadata = schemaRegistry.getLatestSchemaMetadata(schemaName)
      val schema = schemaRegistry.getById(schemaMetadata.getId)

      schema
    } catch {
      case e: Exception =>
        logger.error("Not found schema on schema registry")
        throw e
    }
  }

}
