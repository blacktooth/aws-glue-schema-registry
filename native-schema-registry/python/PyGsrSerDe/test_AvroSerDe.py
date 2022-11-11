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
        parsed_schema = ser.parse_schema(schema)

        complex_table = [{'name': 'Bob', 'friends': 42, 'age': 33, 'dominant hand': 'right',
                          'allergies': ['apples', 'strawberries']},
                         {'name': 'Jim', 'friends': 13, 'age': 69, 'dominant hand': 'right', 'allergies': []},
                         {'name': 'Joe', 'friends': 86, 'age': 17, 'dominant hand': 'left',
                          'allergies': ['bananas', 'strawberries']},
                         {'name': 'Ted', 'friends': 23, 'age': 51, 'dominant hand': 'right', 'allergies': []}]

        complex_schema = {
            'doc': 'Some people records.',
            'name': 'People',
            'namespace': 'test',
            'type': 'record',
            'fields': [
                {'name': 'name', 'type': 'string'},
                {'name': 'friends', 'type': 'int'},
                {'name': 'age', 'type': 'int'},
                {'name': 'dominant hand', 'type': {
                    "type": "enum",
                    "name": "dominant hand",
                    "symbols": ["right", "left"]
                }, "default": "right"
                 },
                {'name': 'allergies', 'type': {
                    'type': 'array',
                    'items': 'string'
                }
                 }
            ]
        }
        parsed_complex_schema = ser.parse_schema(complex_schema)

        empty_table = []

        empty_schema = {}

        # input validation
        self.assertRaises(ValueError, ser.serialize, 'not a record', parsed_schema)
        self.assertRaises(ValueError, ser.serialize, table, 'not a schema')
        self.assertRaises(KeyError, ser.serialize, table, empty_schema)

        self.assertEqual(None, ser.serialize(None, parsed_schema))
        self.assertEqual(None, ser.serialize(table, None))
        self.assertEqual(None, dsr.deserialize(None, None))

        self.assertTrue(len(ser.serialize(empty_table, parsed_schema)) != 0)

        # test functionality
        test_bytes = ser.serialize(table, parsed_schema)
        self.assertTrue(len(test_bytes) != 0)

        decoded = dsr.deserialize(test_bytes, parsed_schema)
        self.assertTrue(len(decoded) != 0)

        self.assertEqual(decoded, table)

        # test complex functionality
        test_bytes = ser.serialize(complex_table, parsed_complex_schema)
        self.assertTrue(len(test_bytes) != 0)

        decoded = dsr.deserialize(test_bytes, parsed_complex_schema)
        self.assertTrue(len(decoded) != 0)

        self.assertEqual(decoded, complex_table)
