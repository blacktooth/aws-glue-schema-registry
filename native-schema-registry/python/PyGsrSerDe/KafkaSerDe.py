from .PyGsrSerDe import *
from .AvroSerDe import *

class GlueSchemaRegistryKafkaSerializer:
    def __init__(self):  # data format design needs discussion
        self.data_format = "AVRO"
        serializers = {
            "AVRO": AvroSerializer(),
            "JSON": "JSONSerializer()",  # placeholders
            "PROTOBUF": "ProtobufSerializer()",
        }
        self.serializer = serializers.get(self.data_format)

    def serialize(self, data, schema, topic):
        if data is None:
            return None
        if schema is None:
            return None

        serialized_record = self.serializer.serialize(data, schema)

        gsr_schema = GlueSchemaRegistrySchema("schema_name", schema, self.data_format)
        gsr_serializer = GlueSchemaRegistrySerializer

        encoded = gsr_serializer.encode(topic, gsr_schema, serialized_record)

        return encoded

class GlueSchemaRegistryKafkaDeserializer:
    def __init__(self):  # data format design needs discussion
        self.data_format = "AVRO"
        deserializers = {
            "AVRO": AvroDeserializer(),
            "JSON": "JSONDeserializer()",  # placeholders
            "PROTOBUF": "ProtobufDeserializer()",
        }
        self.deserializer = deserializers.get(self.data_format)

    def deserialize(self, data, schema):
        if data is None:
            return None
        if schema is None:
            return None

        record_bytes = self.deserializer.deserialize(data, schema)

        gsr_deserializer = GlueSchemaRegistryDeserializer

        decoded = gsr_deserializer.decode(record_bytes)

        return decoded
