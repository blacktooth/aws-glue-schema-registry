import fastavro
from io import BytesIO


class AvroSerializer:

    def parse_schema(self, schema):
        return fastavro.parse_schema(schema)

    def serialize(self, data: dict, schema: dict):
        if data is None:
            return None
        fo = BytesIO()
        fastavro.writer(fo, schema, data)
        return fo.getvalue()


class AvroDeserializer:

    def __init__(self):
        pass

    def deserialize(self, bytes: bytes, schema: dict):
        if bytes is None:
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
    """
    Generic runtime exception to throw in case of serialization / de-serialization exceptions
    """
    pass
