from .PyGsrSerDe import *
from .AvroSerDe import *

class GlueSchemaRegistryKafkaSerializer:
    def __init__(self):  # data format design needs discussion
        self.data_format = "AVRO"
        self.gsr_serializer = GlueSchemaRegistrySerializer
        serializers = {
            "AVRO": AvroSerializer(),
            "JSON": "JSONSerializer()",  # placeholders
            "PROTOBUF": "ProtobufSerializer()",
        }
        self.data_format_serializer = serializers.get(self.data_format)

    def serialize(self, data: dict, schema: dict, topic: str):
        if data is None:
            return None
        if schema is None:
            return None

        serialized_record = self.data_format_serializer.serialize(data, schema)

        gsr_schema = GlueSchemaRegistrySchema("schema_name", schema, self.data_format)

        encoded = self.gsr_serializer.encode(topic, gsr_schema, serialized_record)

        return encoded

class GlueSchemaRegistryKafkaDeserializer:
    def __init__(self):  # data format design needs discussion
        self.data_format = "AVRO"
        self.gsr_deserializer = GlueSchemaRegistryDeserializer
        deserializers = {
            "AVRO": AvroDeserializer(),
            "JSON": "JSONDeserializer()",  # placeholders
            "PROTOBUF": "ProtobufDeserializer()",
        }
        self.data_format_deserializer = deserializers.get(self.data_format)

    def deserialize(self, data: bytes):
        if data is None:
            return None

        record_bytes = self.gsr_deserializer.decode(data)
        schema = self.gsr_deserializer.decode_schema(data)

        decoded = self.data_format_deserializer.deserialize(record_bytes, schema)

        return decoded
