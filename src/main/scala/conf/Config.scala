package conf

import enums.Serializer

final case class Config (brokersList: String, schemaRegistryUrl: String, schemaName: String,
                         topicName: String, numberRecord: Int, hasKey: Boolean,
                         serializerType: Serializer, fieldsWithCustomValue: String)


