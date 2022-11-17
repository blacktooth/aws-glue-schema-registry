import unittest
from KafkaSerDe import *


class TestExample(unittest.TestCase):
    def test_example(self):
        # sample test method to check if unittests are working
        self.assertEqual('a', 'a')


class TestKafkaSerDe(unittest.TestCase):

    def test_serde(self):

        table = [{'name': 'Bob', 'friends': 42, 'age': 33},
                 {'name': 'Jim', 'friends': 13, 'age': 69},
                 {'name': 'Joe', 'friends': 86, 'age': 17},
                 {'name': 'Ted', 'friends': 23, 'age': 51}]

        schema = {
            'doc': 'Some people records.',
            'name': 'People',
            'namespace': 'test',
            'type': 'record',
            'fields': [
                {'name': 'name', 'type': 'string'},
                {'name': 'friends', 'type': 'int'},
                {'name': 'age', 'type': 'int'},
            ]
        }

        config = {
            'schema': schema,
            'topic': 'placeholder topic',
            'dataformat': 'AVRO'
        }

        ser = GlueSchemaRegistryKafkaSerializer(config)
        dsr = GlueSchemaRegistryKafkaDeserializer(config)

        #test functionality for kafka python
        bytes = ser.serialize_for_kafka_python_client(table)
        self.assertTrue(len(bytes) != 0)

        decoded = dsr.deserialize_for_kafka_python_client(bytes)
        self.assertTrue(len(decoded) != 0)

        self.assertEqual(decoded, table)

        # test functionality for confluent
        bytes = ser.serialize_for_confluent_python_kafka_client(table, '')
        self.assertTrue(len(bytes) != 0)

        decoded = dsr.deserialize_for_confluent_python_kafka_client('serializationcontext', bytes)
        self.assertTrue(len(decoded) != 0)

        self.assertEqual(decoded, table)
