from .PyGsrSerDe import *
from .AvroSerDe import *
from .GlueSchemaRegistryConfiguration import *


class GlueSchemaRegistryKafkaSerializer:
    """A serializer to process and serialize kafka records."""

    def __init__(self, configs: dict):
        """Create a GlueSchemaRegistryKafkaSerializer object.

        Args:
            configs -- a dictionary containing necessary configuration properties, such as dataformat, schema, and schema_naming_strategy

        Returns:
            a GlueSchemaRegistryKafkaSerializer object.
        """
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

    def serialize(self, topic: str, data: dict):
        """Process and serialize a record.

        Args:
            topic -- the name of the topic.
            data -- the record to be serialized, as a dictionary.

        Returns:
            the encoded record, along with the topic and schema, as serialized bytes.
        """
        if data is None:
            return None

        serialized_record = self.data_format_serializer.serialize(data, self.config.schema)

        schema_name = self.config.schema_naming_strategy.getSchemaName(data, topic)
        gsr_schema = GlueSchemaRegistrySchema(schema_name, self.config.schema, self.config.dataformat)

        encoded = self.gsr_serializer.encode(topic, gsr_schema, serialized_record)

        return encoded

    def serialize_for_kafka_python_client(self, data: dict):
        """A wrapper for the serialize method, intended as a callable for kafka python client consumer.

        Args:
            data -- the record to be serialized, as a dictionary.

        Returns:
            the encoded record, along with the topic and schema, as serialized bytes.
        """
        return self.serialize(self.config.topic, data)

    def serialize_for_confluent_python_kafka_client(self, data: dict, serializationcontext: object = None):
        """A wrapper for the serialize method, intended as a callable for confluent python kafka client consumer.

        Args:
            data -- the record to be serialized, as a dictionary.

        Returns:
            the encoded record, along with the topic and schema, as serialized bytes.
        """
        return self.serialize(serializationcontext.topic, data)


class GlueSchemaRegistryKafkaDeserializer:
    """A deserializer to process and deserialize bytes into records."""

    def __init__(self, configs: dict):
        """Create a GlueSchemaRegistryDeserializer object.

        Args:
            configs -- a dictionary containing necessary configuration properties, such as dataformat and schema

        Returns:
            a GlueSchemaRegistryKafkaDeserializer object.
        """
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

    def deserializer(self, data: bytes):
        """Deserialize and process bytes into a record.

        Args:
            data -- the bytes to be deserialized into a record.

        Returns:
            the decoded record.
        """
        if data is None:
            return None

        record_bytes = self.gsr_deserializer.decode(data)
        schema = self.gsr_deserializer.decode_schema(data)

        decoded = self.data_format_deserializer.deserialize(record_bytes, schema)

        return decoded

    def deserialize_for_kafka_python_client(self, data: bytes):
        """A wrapper for the deserialize method, intended as a callable for kafka python client consumer.

        Args:
            data -- the bytes to be deserialized into a record.

        Returns:
            the decoded record.
        """
        return self.deserializer(data)

    def deserialize_for_confluent_python_kafka_client(self, serializationcontext:object, data: bytes):
        """A wrapper for the deserialize method, intended as a callable for confluent python kafka client consumer.

        Args:
            data -- the bytes to be deserialized into a record.

        Returns:
            the decoded record.
        """
        return self.deserializer(data)
