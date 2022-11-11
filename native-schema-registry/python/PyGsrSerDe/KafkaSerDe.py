from .PyGsrSerDe import *
from .AvroSerDe import *
from .GlueSchemaRegistryConfiguration import *


class GlueSchemaRegistryKafkaSerializer:
    """A serializer to process and serialize kafka records."""

    def __init__(self, configs: dict):
        """Create a GlueSchemaRegistryKafkaSerializer object."""
        self.config = GlueSchemaRegistryConfiguration(configs)
        self.gsr_serializer = GlueSchemaRegistrySerializer
        serializers = {
            "AVRO": self.getAvroSerializer(),
            "JSON": "self.getJSONSerializer()",  # placeholders
            "PROTOBUF": "self.getProtobufSerializer()",
        }
        self.data_format_serializer = serializers.get(self.config.dataformat)

    def getAvroSerializer(self):
        """Get method for AvroSerializer. Prevents having to initialize unused serializers."""
        return AvroSerializer()

    def serialize_for_kafka_python(self, data: dict):
        """Process and serialize a record."""
        if data is None:
            return None

        serialized_record = self.data_format_serializer.serialize(data, self.config.schema)

        schema_name = self.config.schema_naming_strategy.getSchemaName(data, self.config.topic)
        gsr_schema = GlueSchemaRegistrySchema(schema_name, self.config.schema, self.config.dataformat)

        encoded = self.gsr_serializer.encode(self.config.topic, gsr_schema, serialized_record)

        return encoded

    def serialize_for_confluent(self, data: dict, serializationcontext: object = None):
        """Process and serialize a record, with serializationcontext as an additional input."""
        if data is None:
            return None

        serialized_record = self.data_format_serializer.serialize(data, self.config.schema)

        schema_name = self.config.schema_naming_strategy.getSchemaName(data, serializationcontext.topic)
        gsr_schema = GlueSchemaRegistrySchema(schema_name, self.config.schema, self.config.dataformat)

        encoded = self.gsr_serializer.encode(serializationcontext.topic, gsr_schema, serialized_record)

        return encoded


class GlueSchemaRegistryKafkaDeserializer:
    """A deserializer to process and deserialize bytes into records."""

    def __init__(self, configs: dict):
        """Create a GlueSchemaRegistryDeserializer object."""
        self.config = GlueSchemaRegistryConfiguration(configs)
        self.gsr_deserializer = GlueSchemaRegistryDeserializer
        deserializers = {
            "AVRO": self.getAvroDeserializer(),
            "JSON": "self.getJSONDeserializer()",  # placeholders
            "PROTOBUF": "self.getProtobufDeserializer()",
        }
        self.data_format_deserializer = deserializers.get(self.config.dataformat)

    def getAvroDeserializer(self):
        """Get method for AvroDeserializer. Prevents having to initialize unused deserializers."""
        return AvroDeserializer()

    def deserialize_for_kafka_python(self, data: bytes):
        """Deserialize and process bytes into a record."""
        if data is None:
            return None

        record_bytes = self.gsr_deserializer.decode(data)
        schema = self.gsr_deserializer.decode_schema(data)

        decoded = self.data_format_deserializer.deserialize(record_bytes, schema)

        return decoded

    def deserialize_for_confluent(self, serializationcontext:object, data: bytes):
        """Deserialize and process bytes into a record, with serializationcontext as an additional input."""
        if data is None:
            return None

        record_bytes = self.gsr_deserializer.decode(data)
        schema = self.gsr_deserializer.decode_schema(data)

        decoded = self.data_format_deserializer.deserialize(record_bytes, schema)

        return decoded
