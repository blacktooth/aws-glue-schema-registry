import fastavro
from io import BytesIO


class AvroSerializer:
    """A serializer for serializing AVRO records into bytes."""

    # NOTE(allkliu): Parsing the schema manually is not necessary, as the fastavro.writer
    # will parse it automatically. However, parsing and saving the schema
    # saves time if calling serialize repeatedly.
    def parse_schema(self, schema: dict):
        """Parse and return the schema."""
        return fastavro.parse_schema(schema)

    def serialize(self, data: dict, schema: dict):
        """Serialize a record and return the serialized bytes."""
        if (data is None) or (schema is None):
            return None
        fo = BytesIO()
        fastavro.writer(fo, schema, data)
        return fo.getvalue()


class AvroDeserializer:
    """A deserializer for deserializing bytes into AVRO records."""

    def deserialize(self, bytes: bytes, schema: dict):
        """Deserialize bytes into a record."""
        if (bytes is None) or (schema is None):
            return None
        try:
            fo = BytesIO(bytes)
            avro_reader = fastavro.reader(fo, schema)
            fullrecord = []
            for record in avro_reader:
                fullrecord.append(record)
            return fullrecord

        except Exception as e:
            raise AvroSerdeException(e)


class AvroSerdeException(Exception):
    """Generic runtime exception to throw in case of serialization / de-serialization exceptions."""

    pass
