import unittest
from KafkaSerDe import *


class TestExample(unittest.TestCase):
    def test_example(self):
        # sample test method to check if unittests are working
        self.assertEqual('a', 'a')


class TestKafkaSerDe(unittest.TestCase):

    def test_serde(self):
        ser = GlueSchemaRegistryKafkaSerializer()
        dsr = GlueSchemaRegistryKafkaDeserializer()

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

        bytes = ser.serialize(table, schema, 'topic name')
        self.assertTrue(len(bytes) != 0)

        decoded = dsr.deserialize(bytes, schema)
        self.assertTrue(len(decoded) != 0)

        self.assertEqual(decoded, table)
