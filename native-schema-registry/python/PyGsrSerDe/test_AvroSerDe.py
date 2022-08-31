import unittest
from AvroSerDe import *


class TestExample(unittest.TestCase):
    def test_example(self):
        # sample test method to check if unittests are working
        self.assertEqual('a', 'a')


class TestAvroSerDe(unittest.TestCase):

    def test_serde(self):

        ser = AvroSerializer()
        dsr = AvroDeserializer()

        table = [{'name': 'Bob', 'friends':42, 'age':33},
                 {'name': 'Jim', 'friends':13, 'age':69},
                 {'name': 'Joe', 'friends':86, 'age':17},
                 {'name': 'Ted', 'friends':23, 'age':51}]

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
        parsed_schema = ser.parse_schema(schema)

        # input validation
        self.assertRaises(ValueError, ser.serialize, 'not a record', parsed_schema)
        self.assertRaises(ValueError, ser.serialize, table, 'not a schema')

        # test functionality
        test_bytes = ser.serialize(table, parsed_schema)
        self.assertTrue(len(test_bytes) != 0)

        decoded = dsr.deserialize(test_bytes, parsed_schema)
        self.assertTrue(len(decoded) != 0)

        self.assertEqual(decoded, table)


