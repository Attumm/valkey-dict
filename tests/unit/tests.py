from typing import Any

import sys
import time
import json
import uuid

import unittest

from datetime import datetime, timedelta

import redis

from valkey_dict import ValkeyDict, PythonValkeyDict
from valkey_dict import ValkeyDictJSONEncoder, ValkeyDictJSONDecoder


# !! Make sure you don't have keys within valkey named like this, they will be deleted.
TEST_NAMESPACE_PREFIX = '__test_prefix_key_meta_8128__'

valkey_config = {
    'host': 'localhost',
    'port': 6379,
    'db': 11,
}


def skip_before_python39(test_item):
    """
    Decorator to skip tests for Python versions before 3.9
    where dictionary union operations are not supported.

    Can be used to decorate both test methods and test classes.

    Args:
        test_item: The test method or class to be decorated

    Returns:
        The decorated test item that will be skipped if Python version < 3.9
    """
    reason = "Dictionary union operators (|, |=) require Python 3.9+"

    if sys.version_info < (3, 9):
        if isinstance(test_item, type):
            return unittest.skip(reason)(test_item)
        return unittest.skip(reason)(test_item)
    return test_item


class TestValkeyDictBehaviorDict(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.valkeydb = redis.StrictRedis(**valkey_config)
        cls.r = cls.create_valkey_dict()

    @classmethod
    def tearDownClass(cls):
        cls.clear_test_namespace()

    @classmethod
    def create_valkey_dict(cls, namespace=TEST_NAMESPACE_PREFIX, **kwargs):
        config = valkey_config.copy()
        config.update(kwargs)
        return ValkeyDict(namespace=namespace, **config)

    @classmethod
    def clear_test_namespace(cls):
        cls.valkeydb.flushdb()  # TODO Remove flush make sure everything is deleted.
        cls.valkeydb.delete(f"valkey-dict-insertion-order-{TEST_NAMESPACE_PREFIX}")
        for key in cls.valkeydb.scan_iter('{}:*'.format(TEST_NAMESPACE_PREFIX)):
            cls.valkeydb.delete(key)

    def setUp(self):
        self.clear_test_namespace()

    def _is_python_valkey_dict(self, valkey_dic):
        return getattr(valkey_dic, '_insertion_order_key', None) is not None

    def test_python3_all_methods_from_dictionary_are_implemented(self):
        valkey_dic = self.create_valkey_dict()
        dic = dict()

        self.assertEqual(set(dir({})) - set(dir(ValkeyDict)), set())
        self.assertEqual(len(set(dir(dic)) - set(dir(valkey_dic))), 0)

    def test_input_items(self):
        """Calling ValkeyDict.keys() should return an empty list."""
        valkey_dic = self.create_valkey_dict()
        dic = dict()

        expected = 0
        self.assertEqual(expected, len(dic))
        self.assertEqual(expected, len(valkey_dic))

        expected = 1
        expected_key = 'one item'
        expected_value = 1
        valkey_dic[expected_key] = expected_value
        dic[expected_key] = expected_value
        self.assertEqual(expected, len(dic))
        self.assertEqual(expected, len(valkey_dic))

        self.assertTrue(expected_key in valkey_dic)
        self.assertTrue(expected_key in dic)

        self.assertEqual(dic[expected_key], valkey_dic[expected_key])

        items = (('{} item'.format(i), i) for i in range(1, 5))
        for key, value in items:
            valkey_dic[key] = value
            dic[key] = value

        expected = 5
        self.assertEqual(expected, len(dic))
        self.assertEqual(expected, len(valkey_dic))

        for key, expected_value in items:
            self.assertEqual(dic[key], expected_value)
            self.assertEqual(dic[key], valkey_dic[key])

    def test_supported_types(self):
        valkey_dic = self.create_valkey_dict()
        dic = dict()

        input_values = [
            ("int", 1),
            ("float", 0.9),
            ("str", "im a string"),
            ("bool", True),
            ("None", None),
            ("list", [1, 2, 3]),
            ("dict", {"foo": "bar"}),
        ]

        for key, value in input_values:
            valkey_dic[key] = value
            dic[key] = value

        expected_len = len(input_values)
        self.assertEqual(expected_len, len(valkey_dic))
        self.assertEqual(len(dic), len(valkey_dic))

        for expected_key, expected_value in input_values:
            result = valkey_dic[expected_key]
            self.assertEqual(expected_value, result)
            self.assertEqual(dic[expected_key], result)

        self.assertTrue(len(valkey_dic) > 2)
        valkey_dic.clear()
        dic.clear()
        self.assertEqual(len(valkey_dic), 0)
        self.assertEqual(len(dic), 0)

    def test_update(self):
        valkey_dic = self.create_valkey_dict()
        dic = dict()

        input_values = {
            "int": 1,
            "float": 0.9,
            "str": "im a string",
            "bool": True,
            "None": None,
        }

        self.assertEqual(len(valkey_dic), 0)
        self.assertEqual(len(dic), 0)
        self.assertEqual(len(input_values), 5)

        valkey_dic.update(input_values)
        dic.update(input_values)

        self.assertEqual(len(valkey_dic), 5)
        self.assertEqual(len(dic), 5)
        self.assertEqual(len(input_values), 5)

        for expected_key, expected_value in input_values.items():
            self.assertEqual(valkey_dic[expected_key], expected_value)
            self.assertEqual(dic[expected_key], expected_value)

    def test_iter(self):
        valkey_dic = self.create_valkey_dict()
        dic = dict()

        input_items = {
            "int": 1,
            "float": 0.9,
            "str": "im a string",
            "bool": True,
            "None": None,
        }

        self.assertEqual(len(valkey_dic), 0)
        self.assertEqual(len(dic), 0)
        self.assertEqual(len(input_items), 5)

        valkey_dic.update(input_items)
        dic.update(input_items)

        self.assertEqual(len(valkey_dic), 5)
        self.assertEqual(len(dic), 5)
        self.assertEqual(len(input_items), 5)

        for expected_key, expected_value in input_items.items():
            self.assertEqual(valkey_dic[expected_key], expected_value)
            self.assertEqual(dic[expected_key], expected_value)

        for key in valkey_dic:
            self.assertTrue(key in input_items)

        for key in valkey_dic.keys():
            self.assertTrue(key in input_items)

        for key, value in valkey_dic.items():
            self.assertEqual(input_items[key], value)
            self.assertEqual(dic[key], value)

        input_values = list(input_items.values())
        dic_values = list(dic.values())
        result_values = list(valkey_dic.values())

        self.assertEqual(sorted(map(str, input_values)), sorted(map(str, result_values)))
        self.assertEqual(sorted(map(str, dic_values)), sorted(map(str, result_values)))

        result_values = list(valkey_dic.values())
        self.assertEqual(sorted(map(str, input_values)), sorted(map(str, result_values)))
        self.assertEqual(sorted(map(str, dic_values)), sorted(map(str, result_values)))

    def test_dict_method_update(self):
        valkey_dic = self.create_valkey_dict()
        dic = dict()

        input_items = {
            "int": 1,
            "float": 0.9,
            "str": "im a string",
            "bool": True,
            "None": None,
        }

        valkey_dic.update(input_items)
        dic.update(input_items)

        self.assertEqual(len(valkey_dic), 5)
        self.assertEqual(len(dic), 5)
        self.assertEqual(len(input_items), 5)

    def test_dict_method_pop(self):
        valkey_dic = self.create_valkey_dict()
        dic = dict()

        input_items = {
            "int": 1,
            "float": 0.9,
            "str": "im a string",
            "bool": True,
            "None": None,
        }

        valkey_dic.update(input_items)
        dic.update(input_items)

        self.assertEqual(len(valkey_dic), 5)
        self.assertEqual(len(dic), 5)
        self.assertEqual(len(input_items), 5)

        for i, key in enumerate(input_items.keys(), start=1):
            expected = dic.pop(key)
            result = valkey_dic.pop(key)
            self.assertEqual(expected, result)
            self.assertEqual(len(dic), len(input_items) - i)
            self.assertEqual(len(valkey_dic), len(input_items) - i)

        with self.assertRaises(KeyError):
            dic.pop("item")
        with self.assertRaises(KeyError):
            valkey_dic.pop("item")

    def test_dict_method_pop_default(self):
        valkey_dic = self.create_valkey_dict()
        dic = dict()

        input_items = {
            "int": 1,
            "float": 0.9,
            "str": "im a string",
            "bool": True,
            "None": None,
        }

        valkey_dic.update(input_items)
        dic.update(input_items)

        self.assertEqual(len(valkey_dic), 5)
        self.assertEqual(len(dic), 5)
        self.assertEqual(len(input_items), 5)

        for i, key in enumerate(input_items.keys(), start=1):
            expected = dic.pop(key)
            result = valkey_dic.pop(key)
            self.assertEqual(expected, result)
            self.assertEqual(len(dic), len(input_items) - i)
            self.assertEqual(len(valkey_dic), len(input_items) - i)

        expected = "default item"
        self.assertEqual(dic.pop("item", expected), expected)
        self.assertEqual(valkey_dic.pop("item", expected), expected)

        expected = None
        self.assertEqual(dic.pop("item", expected), expected)
        self.assertEqual(valkey_dic.pop("item", expected), expected)

    def test_dict_method_popitem(self):
        valkey_dic = self.create_valkey_dict()
        dic = dict()

        input_items = {
            "int": 1,
            "float": 0.9,
            "str": "im a string",
            "bool": True,
            "None": None,
        }

        valkey_dic.update(input_items)
        dic.update(input_items)

        self.assertEqual(len(valkey_dic), 5)
        self.assertEqual(len(dic), 5)
        self.assertEqual(len(input_items), 5)

        expected = [dic.popitem() for _ in range(5)]
        result = [valkey_dic.popitem() for _ in range(5)]

        self.assertEqual(sorted(map(str, expected)), sorted(map(str, result)))

        self.assertEqual(len(dic), 0)
        self.assertEqual(len(valkey_dic), 0)

        with self.assertRaises(KeyError):
            dic.popitem()
        with self.assertRaises(KeyError):
            valkey_dic.popitem()

    def test_dict_method_popitem_dict_compliant(self):
        valkey_dic = self.create_valkey_dict()

        if self._is_python_valkey_dict(valkey_dic):
            return

        dic = dict()

        input_items = {
            "int": 1,
            "float": 0.9,
            "str": "im a string",
            "bool": True,
            "None": None,
        }

        valkey_dic.update(input_items)
        dic.update(input_items)

        self.assertEqual(len(valkey_dic), 5)
        self.assertEqual(len(dic), 5)
        self.assertEqual(len(input_items), 5)

        self.assertEqual(list(dic), list(valkey_dic))
        self.assertEqual(list(dic.keys()), list(valkey_dic.keys()))
        self.assertEqual(list(dic.values()), list(valkey_dic.values()))
        self.assertEqual(list(dic.items()), list(valkey_dic.items()))

        for i in range(5):
            expected = dic.popitem()
            result = valkey_dic.popitem()
            self.assertEqual(expected, result)

        self.assertEqual(list(dic), list(valkey_dic))

        self.assertEqual(len(dic), 0)
        self.assertEqual(len(valkey_dic), 0)

        with self.assertRaises(KeyError):
            dic.popitem()
        with self.assertRaises(KeyError):
            valkey_dic.popitem()

    @skip_before_python39
    def test_dict_method_or(self):
        valkey_dic = self.create_valkey_dict()
        dic = dict()

        input_items = {
            "int": 1,
            "float": 0.9,
            "str": "im a string",
            "bool": True,
            "None": None,
        }

        additional_items = {
            "str": "new string",
            "new_int": 42,
            "new_bool": False,
        }

        valkey_dic.update(input_items)
        dic.update(input_items)

        self.assertEqual(len(valkey_dic), 5)
        self.assertEqual(len(dic), 5)
        self.assertEqual(len(input_items), 5)

        valkey_result = valkey_dic | additional_items
        dict_result = dic | additional_items

        self.assertEqual(len(valkey_result), len(dict_result))
        self.assertEqual(dict(valkey_result), dict_result)

        self.assertEqual(len(valkey_dic), 5)
        self.assertEqual(len(dic), 5)
        self.assertEqual(dict(valkey_dic), dict(dic))

        with self.assertRaises(TypeError):
            dic | [1, 2]

        with self.assertRaises(TypeError):
            valkey_dic | [1, 2]

    @skip_before_python39
    def test_dict_method_ror(self):
        valkey_dic = self.create_valkey_dict()
        dic = dict()

        input_items = {
            "int": 1,
            "float": 0.9,
            "str": "im a string",
            "bool": True,
            "None": None,
        }

        additional_items = {
            "str": "new string",
            "new_int": 42,
            "new_bool": False,
        }

        valkey_dic.update(input_items)
        dic.update(input_items)

        self.assertEqual(len(valkey_dic), 5)
        self.assertEqual(len(dic), 5)
        self.assertEqual(len(input_items), 5)

        valkey_result = additional_items | valkey_dic
        dict_result = additional_items | dic

        self.assertEqual(len(valkey_result), len(dict_result))
        self.assertEqual(dict(valkey_result), dict_result)

        # Verify original dicts weren't modified
        self.assertEqual(len(valkey_dic), 5)
        self.assertEqual(len(dic), 5)
        self.assertEqual(dict(valkey_dic), dict(dic))

        with self.assertRaises(TypeError):
            [1, 2] | dic

        with self.assertRaises(TypeError):
             [1, 2] | valkey_dic

    @skip_before_python39
    def test_dict_method_ior(self):
        valkey_dic = self.create_valkey_dict()
        dic = dict()

        input_items = {
            "int": 1,
            "float": 0.9,
            "str": "im a string",
            "bool": True,
            "None": None,
        }

        additional_items = {
            "str": "new string",
            "new_int": 42,
            "new_bool": False,
        }

        valkey_dic.update(input_items)
        dic.update(input_items)

        self.assertEqual(len(valkey_dic), 5)
        self.assertEqual(len(dic), 5)
        self.assertEqual(len(input_items), 5)

        valkey_dic |= additional_items
        dic |= additional_items

        self.assertEqual(len(valkey_dic), len(dic))
        self.assertEqual(dict(valkey_dic), dict(dic))

        with self.assertRaises(TypeError):
            dic |= [1, 2]

        with self.assertRaises(TypeError):
            valkey_dic |= [1, 2]

    def test_dict_method_reversed_(self):
        """
        ValkeyDict without the flag dict_compliant insertion order doens't make use of insert_order.
        This test only test `reversed` can be called.
        """
        valkey_dic = self.create_valkey_dict()
        dic = dict()

        input_items = {
            "int": 1,
            "bool": True,
            "None": None,
        }

        valkey_dic.update(input_items)
        dic.update(input_items)

        if not self._is_python_valkey_dict(valkey_dic):
            self.assertEqual(list(dic), list(valkey_dic))
            self.assertEqual(list(reversed(dic)), list(reversed(valkey_dic)))

        valkey_reversed = sorted(reversed(valkey_dic))
        dict_reversed = sorted(reversed(dic))
        self.assertEqual(valkey_reversed, dict_reversed)

    def test_dict_method_class_getitem(self):
        valkey_dic = self.create_valkey_dict()
        dic = dict()

        input_items = {
            "int": 1,
            "float": 0.9,
            "str": "im a string",
            "bool": True,
            "None": None,
        }

        valkey_dic.update(input_items)
        dic.update(input_items)

        self.assertEqual(len(valkey_dic), 5)
        self.assertEqual(len(dic), 5)
        self.assertEqual(len(input_items), 5)

        def accepts_valkey_dict(d: ValkeyDict[str, Any]) -> None:
            self.assertIsInstance(d, ValkeyDict)

        accepts_valkey_dict(valkey_dic)

    def test_dict_method_setdefault(self):
        valkey_dic = self.create_valkey_dict()
        dic = dict()

        dic.setdefault("item", 4)
        valkey_dic.setdefault("item", 4)

        self.assertEqual(dic["item"], valkey_dic["item"])

        self.assertEqual(len(dic), 1)
        self.assertEqual(len(valkey_dic), 1)

        dic.setdefault("item", 5)
        valkey_dic.setdefault("item", 5)

        self.assertEqual(dic["item"], valkey_dic["item"])

        self.assertEqual(len(dic), 1)
        self.assertEqual(len(valkey_dic), 1)

        dic.setdefault("foobar", 6)
        valkey_dic.setdefault("foobar", 6)

        self.assertEqual(dic["item"], valkey_dic["item"])
        self.assertEqual(dic["foobar"], valkey_dic["foobar"])

        self.assertEqual(len(dic), 2)
        self.assertEqual(len(valkey_dic), 2)

    def test_dict_method_setdefault_with_expire(self):
        """Test setdefault with expiration setting"""
        valkey_dic = self.create_valkey_dict(expire=3600)
        key = "test_expire_key"
        expected_value = "expected value"
        other_expected_value = "other_default_value"

        # Clear any existing values
        valkey_dic.clear()

        # First call - should set with expiry
        result_one = valkey_dic.setdefault(
            key, expected_value
        )
        self.assertEqual(result_one, expected_value)
        # Check TTL
        actual_ttl = valkey_dic.get_ttl(key)
        self.assertAlmostEqual(3600, actual_ttl, delta=2)

        # Second call - should get existing value and maintain TTL
        time.sleep(1)
        result_two = valkey_dic.setdefault(
            key, other_expected_value,
        )
        self.assertEqual(result_one, expected_value)
        self.assertNotEqual(result_two, other_expected_value)
        # TTL should be ~1 second less
        new_ttl = valkey_dic.get_ttl(key)
        self.assertAlmostEqual(3600 - 1, new_ttl, delta=2)

        # Value should be unchanged
        self.assertEqual(result_one, result_two)

        self.assertEqual(expected_value, valkey_dic[key])
        del valkey_dic[key]
        with valkey_dic.expire_at(timedelta(seconds=1)):
            result_one_three = valkey_dic.setdefault(
                key, other_expected_value,
            )
            self.assertEqual(other_expected_value, valkey_dic[key])
        time.sleep(1.5)
        with self.assertRaisesRegex(KeyError, key):
            valkey_dic[key]

    def test_setdefault_with_preserve_ttl(self):
        """Test setdefault with preserve_expiration=True"""
        valkey_dic = self.create_valkey_dict(expire=5, preserve_expiration=True, namespace=str(uuid.uuid4()))
        key = f"test_preserve_key_{str(uuid.uuid4())}"
        expected_value = "expected_value"
        default_value = "default"
        sleep_time = 2

        valkey_dic[key] = expected_value
        initial_ttl = valkey_dic.get_ttl(key)

        time.sleep(sleep_time)
        # Try setdefault - should keep original TTL
        result = valkey_dic.setdefault(
            key, default_value
        )
        self.assertEqual(result, expected_value)

        time.sleep(sleep_time)
        # TTL should have been preserved, thus new_ttl+sleep_time should less than initial_ttl since sleep 1 second.
        new_ttl = valkey_dic.get_ttl(key)
        self.assertLess(new_ttl + sleep_time, initial_ttl)
        time.sleep(sleep_time)

        # TTL should be expired, thus key and value should be missing, and thus we will set the default value.
        with self.assertRaisesRegex(KeyError, key):
            valkey_dic[key]

        expected_value_two = "expected_value_two"
        result_two = valkey_dic.setdefault(
            key, expected_value_two
        )
        self.assertEqual(result_two, expected_value_two)
        self.assertEqual(valkey_dic[key], expected_value_two)

    def test_setdefault_concurrent_ttl(self):
        """Test TTL behavior with concurrent setdefault operations"""
        valkey_dic =  self.create_valkey_dict(expire=3600)
        other_valkey_dic =   self.create_valkey_dict(expire=1800)  # Different TTL

        key = "test_concurrent_key"
        default_value = "default"
        other_default_value = "other_default"

        valkey_dic.clear()

        # First operation sets with 3600s TTL
        value1 = valkey_dic.setdefault(
            key, default_value
        )

        ttl1 = valkey_dic.get_ttl(key)
        self.assertAlmostEqual(3600, ttl1, delta=2)

        # Competing operation tries with 1800s TTL
        value2 = other_valkey_dic.setdefault(
        key, other_default_value
        )

        # Original TTL should be maintained
        ttl2 = other_valkey_dic.get_ttl(key)
        self.assertAlmostEqual(3600, ttl2, delta=3)
        self.assertEqual(value1, value2)  # Should get same value

    def test_dict_method_get(self):
        valkey_dic = self.create_valkey_dict()
        dic = dict()

        dic.setdefault("item", 4)
        valkey_dic.setdefault("item", 4)

        self.assertEqual(dic["item"], valkey_dic["item"])

        self.assertEqual(len(dic), 1)
        self.assertEqual(len(valkey_dic), 1)

        self.assertEqual(dic.get("item"), valkey_dic.get("item"))
        self.assertEqual(dic.get("foobar"), valkey_dic.get("foobar"))
        self.assertEqual(dic.get("foobar", "foobar"), valkey_dic.get("foobar", "foobar"))

    def test_dict_method_clear(self):
        valkey_dic = self.create_valkey_dict()
        dic = dict()

        input_items = {
            "int": 1,
            "float": 0.9,
            "str": "im a string",
            "bool": True,
            "None": None,
        }

        valkey_dic.update(input_items)
        dic.update(input_items)

        self.assertEqual(len(valkey_dic), 5)
        self.assertEqual(len(dic), 5)

        dic.clear()
        valkey_dic.clear()

        self.assertEqual(len(valkey_dic), 0)
        self.assertEqual(len(dic), 0)

        # Boundary check. clear on empty dictionary is valid
        dic.clear()
        valkey_dic.clear()

        self.assertEqual(len(valkey_dic), 0)
        self.assertEqual(len(dic), 0)

    def test_dict_method_clear_1(self):
        valkey_dic = self.create_valkey_dict()
        dic = dict()

        input_items = {
            "int": 1,
            "float": 0.9,
            "str": "im a string",
            "bool": True,
            "None": None,
        }

        valkey_dic.update(input_items)
        dic.update(input_items)

        self.assertEqual(len(valkey_dic), 5)
        self.assertEqual(len(dic), 5)

        dic_id = id(dic)
        valkey_dic_id = id(id)

        dic_copy = dic.copy()
        valkey_dic_copy = valkey_dic.copy()

        self.assertNotEqual(dic_id, id(dic_copy))
        self.assertNotEqual(valkey_dic_id, id(valkey_dic_copy))

        dic.clear()
        valkey_dic.clear()

        self.assertEqual(len(valkey_dic), 0)
        self.assertEqual(len(dic), 0)

        self.assertEqual(len(dic_copy), 5)
        self.assertEqual(len(valkey_dic_copy), 5)

    def test_dict_exception_keyerror(self):
        valkey_dic = self.create_valkey_dict()
        dic = dict()

        with self.assertRaisesRegex(KeyError, "appel"):
            dic['appel']
        with self.assertRaisesRegex(KeyError, "appel"):
            valkey_dic['appel']

        with self.assertRaisesRegex(KeyError, r"popitem\(\): dictionary is empty"):
            dic.popitem()
        with self.assertRaisesRegex(KeyError, r"popitem\(\): dictionary is empty"):
            valkey_dic.popitem()

    def test_dict_types_bool(self):
        valkey_dic = self.create_valkey_dict()
        dic = dict()

        input_items = {
            "True": True,
            "False": False,
            "NotTrue": False,
            "NotFalse": True,
        }

        valkey_dic.update(input_items)
        dic.update(input_items)

        self.assertEqual(len(valkey_dic), len(input_items))
        self.assertEqual(len(dic), len(input_items))

        for key, expected_value in input_items.items():
            self.assertEqual(valkey_dic[key], expected_value)
            self.assertEqual(dic[key], expected_value)

        dic.clear()
        valkey_dic.clear()

        self.assertEqual(len(valkey_dic), 0)
        self.assertEqual(len(dic), 0)

        for k, v in input_items.items():
            valkey_dic[k] = v
            dic[k] = v

        for key, expected_value in input_items.items():
            self.assertEqual(valkey_dic[key], expected_value)
            self.assertEqual(dic[key], expected_value)

        for k, v in valkey_dic.items():
            self.assertEqual(input_items[k], v)
            self.assertEqual(dic[k], v)

        self.assertEqual(len(valkey_dic), len(input_items))
        self.assertEqual(len(dic), len(input_items))

    def test_dict_method_pipeline(self):
        valkey_dic = self.create_valkey_dict()
        expected = {
            'a': 1,
            'b': 2,
            'c': 3,
        }
        with valkey_dic.pipeline():
            for k, v in expected.items():
                valkey_dic[k] = v

        self.assertEqual(len(valkey_dic), len(expected))
        for k, v in expected.items():
            self.assertEqual(valkey_dic[k], v)
        valkey_dic.clear()
        self.assertEqual(len(valkey_dic), 0)

        with valkey_dic.pipeline():
            with valkey_dic.pipeline():
                for k, v in expected.items():
                    valkey_dic[k] = v

        self.assertEqual(len(valkey_dic), len(expected))
        for k, v in expected.items():
            self.assertEqual(valkey_dic[k], v)
        valkey_dic.clear()
        self.assertEqual(len(valkey_dic), 0)

        with valkey_dic.pipeline():
            with valkey_dic.pipeline():
                with valkey_dic.pipeline():
                    for k, v in expected.items():
                        valkey_dic[k] = v

        self.assertEqual(len(valkey_dic), len(expected))
        for k, v in expected.items():
            self.assertEqual(valkey_dic[k], v)
        valkey_dic.clear()
        self.assertEqual(len(valkey_dic), 0)

        with valkey_dic.pipeline():
            with valkey_dic.pipeline():
                with valkey_dic.pipeline():
                    with valkey_dic.pipeline():
                        for k, v in expected.items():
                            valkey_dic[k] = v

        self.assertEqual(len(valkey_dic), len(expected))
        for k, v in expected.items():
            self.assertEqual(valkey_dic[k], v)
        valkey_dic.clear()
        self.assertEqual(len(valkey_dic), 0)

    def test_dict_method_pipeline_buffer_sets(self):  # noqa: C901
        valkey_dic = self.create_valkey_dict()
        expected = {
            'a': 1,
            'b': 2,
            'c': 3,
        }
        with valkey_dic.pipeline():
            for k, v in expected.items():
                valkey_dic[k] = v
            self.assertEqual(len(valkey_dic), 0)

        self.assertEqual(len(valkey_dic), len(expected))

        for k, v in expected.items():
            self.assertEqual(valkey_dic[k], v)

        with valkey_dic.pipeline():
            for k, v in valkey_dic.items():
                valkey_dic[k] = v * 2
            for k, v in expected.items():
                self.assertEqual(valkey_dic[k], v)

        for k, v in expected.items():
            self.assertEqual(valkey_dic[k], v * 2)
        self.assertEqual(len(valkey_dic), len(expected))

        valkey_dic.clear()
        self.assertEqual(len(valkey_dic), 0)

        with valkey_dic.pipeline():
            with valkey_dic.pipeline():
                for k, v in expected.items():
                    valkey_dic[k] = v
                self.assertEqual(len(valkey_dic), 0)

        self.assertEqual(len(valkey_dic), len(expected))

        for k, v in expected.items():
            self.assertEqual(valkey_dic[k], v)

        with valkey_dic.pipeline():
            with valkey_dic.pipeline():
                for k, v in valkey_dic.items():
                    valkey_dic[k] = v * 2
                for k, v in expected.items():
                    self.assertEqual(valkey_dic[k], v)

        for k, v in expected.items():
            self.assertEqual(valkey_dic[k], v * 2)
        self.assertEqual(len(valkey_dic), len(expected))

        with valkey_dic.pipeline():
            valkey_dic.clear()
            self.assertEqual(len(valkey_dic), len(expected))

        self.assertEqual(len(valkey_dic), 0)

    def test_dict_method_fromkeys(self):
        valkey_dic = self.create_valkey_dict()
        dic = dict()

        keys = ['a', 'b', 'c', 'd']
        expected_dic = {k: None for k in keys}

        result_dic = dic.fromkeys(keys)
        result_valkey_dic = valkey_dic.fromkeys(keys)

        self.assertEqual(len(result_dic), len(keys))
        self.assertEqual(len(result_valkey_dic), len(keys))
        self.assertEqual(len(expected_dic), len(keys))
        for k, v in expected_dic.items():
            self.assertEqual(result_valkey_dic[k], v)
            self.assertEqual(result_dic[k], v)

    def test_dict_method_fromkeys_with_default(self):
        valkey_dic = self.create_valkey_dict()
        dic = dict()

        expected_default = 42
        keys = ['a', 'b', 'c', 'd']
        expected_dic = {k: expected_default for k in keys}

        result_dic = dic.fromkeys(keys, expected_default)
        result_valkey_dic = valkey_dic.fromkeys(keys, expected_default)

        self.assertEqual(len(result_dic), len(keys))
        self.assertEqual(len(result_valkey_dic), len(keys))
        self.assertEqual(len(expected_dic), len(keys))
        for k, v in expected_dic.items():
            self.assertEqual(result_valkey_dic[k], v)


class TestPythonValkeyDictBehaviorDict(TestValkeyDictBehaviorDict):
    @classmethod
    def create_valkey_dict(cls, namespace=TEST_NAMESPACE_PREFIX, **kwargs):
        config = valkey_config.copy()
        config.update(kwargs)
        return PythonValkeyDict(namespace=namespace+"_PythonValkeyDict", **config)

    def test_dict_method_update_reversed(self):
        """
        PythonValkeyDict Currently support insertion order with
        """
        valkey_dic = self.create_valkey_dict()
        dic = dict()

        input_items = {
            "int": 1,
            "float": 0.9,
            "str": "im a string",
            "bool": True,
            "None": None,
        }

        valkey_dic.update(input_items)
        dic.update(input_items)

        self.assertEqual(len(valkey_dic), 5)
        self.assertEqual(len(dic), 5)
        self.assertEqual(len(input_items), 5)

        valkey_reversed = list(reversed(valkey_dic))
        dict_reversed = list(reversed(dic))

        self.assertEqual(valkey_reversed, dict_reversed)

    def test_sequential__insertion_order_comparison(self):
        d = {}
        d2 = {}
        rd = self.create_valkey_dict()

        # Testing for identity
        self.assertTrue(d is not d2)
        self.assertTrue(d is not rd)

        # Testing for equality
        self.assertTrue(d == d2)
        self.assertTrue(d == rd)
        self.assertTrue(d.items() == d2.items())
        self.assertTrue(list(d.items()) == list(rd.items()))

        # Insert items in specific order
        items = [
            ("key1", "value1"),
            ("key2", "value2"),
            ("key3", "value3"),
            ("key4", "value4")
        ]

        # Modify d only first
        for k, v in items:
            d[k] = v

        # Testing inequality after insertion
        self.assertTrue(d != d2)
        self.assertTrue(d != rd)
        self.assertTrue(d.items() != d2.items())
        self.assertTrue(list(d.items()) != list(rd.items()))

        # Modify d2 and rd
        for k, v in items:
            d2[k] = v
            rd[k] = v

        # Testing equality after insertion
        self.assertTrue(d == d2)
        self.assertTrue(d == rd)
        self.assertTrue(d.items() == d2.items())
        self.assertTrue(list(d.items()) == list(rd.items()))

        # Test iteration order
        self.assertEqual(list(d.keys()), list(d2.keys()))
        self.assertEqual(list(d.keys()), list(rd.keys()))
        self.assertEqual(list(d.values()), list(d2.values()))
        self.assertEqual(list(d.values()), list(rd.values()))
        self.assertEqual(list(d.items()), list(d2.items()))
        self.assertEqual(list(d.items()), list(rd.items()))

        # Test order preservation after updates/deletions
        del d["key2"]
        del d2["key2"]
        del rd["key2"]

        d["key5"] = "value5"
        d2["key5"] = "value5"
        rd["key5"] = "value5"

        # Verify order is still preserved
        self.assertEqual(list(d.keys()), list(d2.keys()))
        self.assertEqual(list(d.keys()), list(rd.keys()))

        self.assertEqual(list(d.values()), list(d2.values()))
        self.assertEqual(list(d.values()), list(rd.values()))

        self.assertEqual(list(d.items()), list(d2.items()))
        self.assertEqual(list(d.items()), list(rd.items()))

    def test_sequential_reverse_iteration_comparison(self):
        d = {}
        d2 = {}
        rd = self.create_valkey_dict()

        # Testing for identity
        self.assertTrue(d is not d2)
        self.assertTrue(d is not rd)

        # Testing for equality
        self.assertTrue(d == d2)
        self.assertTrue(d == rd)
        self.assertTrue(d.items() == d2.items())
        self.assertTrue(list(d.items()) == list(rd.items()))

        # Setup with multiple items
        items = [("key1", "value1"), ("key2", "value2"), ("key3", "value3")]
        for k, v in items:
            d[k] = v
            d2[k] = v
            rd[k] = v

        # Test reverse iteration
        self.assertEqual(list(reversed(d)), list(reversed(d2)))
        self.assertEqual(list(reversed(d)), list(reversed(rd)))

    def test_sequential_deletion_comparison(self):
        d = {}
        d2 = {}
        rd =self.create_valkey_dict()

        # Testing for identity
        self.assertTrue(d is not d2)
        self.assertTrue(d is not rd)

        # Testing for equality
        self.assertTrue(d == d2)
        self.assertTrue(d == rd)
        self.assertTrue(d.items() == d2.items())
        self.assertTrue(list(d.items()) == list(rd.items()))

        # Setup initial state
        d["key1"] = "value1"
        d["key2"] = "value2"
        d2["key1"] = "value1"
        d2["key2"] = "value2"
        rd["key1"] = "value1"
        rd["key2"] = "value2"

        # Delete from d only
        del d["key1"]

        # Testing inequality after deletion
        self.assertTrue(d != d2)
        self.assertTrue(d != rd)
        self.assertTrue(d.items() != d2.items())
        self.assertTrue(list(d.items()) != list(rd.items()))

        # Delete from d2 and rd
        del d2["key1"]
        del rd["key1"]

        # Testing equality after deletion
        self.assertTrue(d == d2)
        self.assertTrue(d == rd)
        self.assertTrue(d.items() == d2.items())
        self.assertTrue(list(d.items()) == list(rd.items()))

        # Test deleting nonexistent key raises KeyError
        with self.assertRaises(KeyError):
            del d["nonexistent"]
        with self.assertRaises(KeyError):
            del d2["nonexistent"]
        with self.assertRaises(KeyError):
            del rd["nonexistent"]

        # Test deleting from empty dict
        d.clear()
        d2.clear()
        rd.clear()

        missing_key = "key1"
        with self.assertRaisesRegex(KeyError, f"'{missing_key}'"):
            del d[missing_key]
        with self.assertRaisesRegex(KeyError, f"'{missing_key}'"):
            del d2[missing_key]
        with self.assertRaisesRegex(KeyError, f"'{missing_key}'"):
            del rd[missing_key]

        # Test deleting last item
        d["last"] = "value"
        d2["last"] = "value"
        rd["last"] = "value"

        del d["last"]
        del d2["last"]
        del rd["last"]

        self.assertEqual(len(d), 0)
        self.assertEqual(len(d2), 0)
        self.assertEqual(len(rd), 0)

        # Test deletion maintains equality
        self.assertTrue(d == d2)
        self.assertTrue(d == rd)
        self.assertTrue(d.items() == d2.items())
        self.assertTrue(list(d.items()) == list(rd.items()))


class TestValkeyDict(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.valkeydb = redis.StrictRedis(**valkey_config)
        cls.r = cls.create_valkey_dict()

    @classmethod
    def tearDownClass(cls):
        cls.clear_test_namespace()

    @classmethod
    def create_valkey_dict(cls, namespace=TEST_NAMESPACE_PREFIX, **kwargs):
        config = valkey_config.copy()
        config.update(kwargs)
        return ValkeyDict(namespace=namespace, **config)

    @classmethod
    def clear_test_namespace(cls):
        for key in cls.valkeydb.scan_iter('{}:*'.format(TEST_NAMESPACE_PREFIX)):
            cls.valkeydb.delete(key)

    def setUp(self):
        self.clear_test_namespace()

    def test_get_valkey_info(self):
        """Ensure get_valkey_info() returns a dictionary with Valkey server information."""
        result = self.r.get_valkey_info()
        self.assertIsInstance(result, dict)
        self.assertIn('valkey_version', result)

    def test_sizeof(self):
        """Verify that ValkeyDict's __sizeof__() method returns the correct size."""
        self.r.clear()
        self.r['key'] = 'value'
        expected = self.r.to_dict().__sizeof__()
        result = self.r.__sizeof__()
        self.assertEqual(expected, result)

    def test_keys_empty(self):
        """Calling ValkeyDict.keys() should return an empty Iterator."""
        keys = self.r.keys()
        self.assertEqual(list(keys), [])

    def test_set_and_get_foobar(self):
        """Test setting a key and retrieving it."""
        self.r['foobar'] = 'barbar'

        self.assertEqual(self.r['foobar'], 'barbar')

    def test_set_none_and_get_none(self):
        """Test setting a key with no value and retrieving it."""
        self.r['foobar'] = None

        self.assertIsNone(self.r['foobar'])

    def test_set_and_get_multiple(self):
        """Test setting two different keys with two different values, and reading them."""
        self.r['foobar1'] = 'barbar1'
        self.r['foobar2'] = 'barbar2'

        self.assertEqual(self.r['foobar1'], 'barbar1')
        self.assertEqual(self.r['foobar2'], 'barbar2')

    def test_get_non_existing(self):
        """Test that retrieving a non-existing key raises a KeyError."""
        with self.assertRaises(KeyError):
            _ = self.r['non_existing_key']

    def test_delete(self):
        """Test deleting a key."""
        key = 'foobar_gone'
        expected = 'bar_gone'
        formatted_key = self.r._format_key(key)

        self.r[key] = expected

        # val should be present
        result = self.r._transform(self.valkeydb.get(formatted_key).decode('utf-8'))
        self.assertEqual(result, expected)

        # value should be removed
        del self.r[key]
        self.assertEqual(self.valkeydb.get(formatted_key), None)

    def test_delete_twice_(self):
        """Test deleting a key twice won't raise default mode will in dict_complaint mode"""
        key = 'foobar_gone'
        self.r[key] = 'bar'

        del self.r[key]
        self.assertEqual(self.valkeydb.get(key), None)

        if self.r.raise_key_error_delete:
            with self.assertRaises(KeyError):
                del self.r[key]
        else:
            del self.r[key]

        self.assertEqual(self.valkeydb.get(key), None)

    def test_contains_empty(self):
        """Tests the __contains__ function with no keys set."""
        self.assertFalse('foobar' in self.r)
        self.assertFalse('foobar1' in self.r)
        self.assertFalse('foobar_is_not_found' in self.r)
        self.assertFalse('1' in self.r)

    def test_contains_nonempty(self):
        """Tests the __contains__ function with keys set."""
        self.r['foobar'] = 'barbar'
        self.assertTrue('foobar' in self.r)

    def test_repr_empty(self):
        """Tests the __repr__ function with no keys set."""
        expected_repr = str({})
        actual_repr = repr(self.r)
        self.assertEqual(actual_repr, expected_repr)

    def test_repr_nonempty(self):
        """Tests the __repr__ function with keys set."""
        key = 'foobar'
        val = 'bar'
        self.r[key] = val
        expected = str({key: val})
        result = repr(self.r)
        self.assertEqual(result, expected)

    def test_str_nonempty(self):
        """Tests the __repr__ function with keys set."""
        key = 'foobar'
        val = 'bar'
        self.r[key] = val
        expected = str({key: val})
        result = str(self.r)
        self.assertEqual(result, expected)

    def test_len_empty(self):
        """Tests the __repr__ function with no keys set."""
        self.assertEqual(len(self.r), 0)

    def test_len_nonempty(self):
        """Tests the __repr__ function with keys set."""
        self.r['foobar1'] = 'barbar1'
        self.r['foobar2'] = 'barbar2'
        self.assertEqual(len(self.r), 2)

    def test_to_dict_empty(self):
        """Tests the to_dict function with no keys set."""
        expected_dict = {}
        actual_dict = self.r.to_dict()
        self.assertEqual(actual_dict, expected_dict)

    def test_to_dict_nonempty(self):
        """Tests the to_dict function with keys set."""
        self.r['foobar'] = 'barbaros'
        expected_dict = {u'foobar': u'barbaros'}
        actual_dict = self.r.to_dict()
        self.assertEqual(actual_dict, expected_dict)

    def test_expire_context(self):
        """Test adding keys with an `expire` value by using the contextmanager."""
        with self.r.expire_at(3600):
            self.r['foobar'] = 'barbar'

        actual_ttl = self.valkeydb.ttl('{}:foobar'.format(self.r.namespace))
        self.assertAlmostEqual(3600, actual_ttl, delta=2)

    def test_expire_context_timedelta(self):
        """ Test adding keys with an `expire` value by using the contextmanager. With timedelta as argument. """
        timedelta_one_hour = timedelta(hours=1)
        timedelta_one_minute = timedelta(minutes=1)
        hour_in_seconds = 60 * 60
        minute_in_seconds = 60

        with self.r.expire_at(timedelta_one_hour):
            self.r['one_hour'] = 'one_hour'
        with self.r.expire_at(timedelta_one_minute):
            self.r['one_minute'] = 'one_minute'

        actual_ttl = self.valkeydb.ttl('{}:one_hour'.format(self.r.namespace))
        self.assertAlmostEqual(hour_in_seconds, actual_ttl, delta=2)
        actual_ttl = self.valkeydb.ttl('{}:one_minute'.format(self.r.namespace))
        self.assertAlmostEqual(minute_in_seconds, actual_ttl, delta=2)

    def test_expire_keyword(self):
        """Test adding keys with an `expire` value by using the `expire` config keyword."""
        r = self.create_valkey_dict(expire=3600)

        r['foobar'] = 'barbar'
        actual_ttl = self.valkeydb.ttl('{}:foobar'.format(self.r.namespace))
        self.assertAlmostEqual(3600, actual_ttl, delta=2)

    def test_expire_keyword_timedelta(self):
        """ Test adding keys with an `expire` value by using the `expire` config keyword. With timedelta as argument."""
        timedelta_one_hour = timedelta(hours=1)
        timedelta_one_minute = timedelta(minutes=1)
        hour_in_seconds = 60 * 60
        minute_in_seconds = 60

        r_hour = self.create_valkey_dict(expire=timedelta_one_hour)
        r_minute = self.create_valkey_dict(expire=timedelta_one_minute)

        r_hour['one_hour'] = 'one_hour'
        r_minute['one_minute'] = 'one_minute'

        actual_ttl = self.valkeydb.ttl('{}:one_hour'.format(self.r.namespace))
        self.assertAlmostEqual(hour_in_seconds, actual_ttl, delta=4)
        actual_ttl = self.valkeydb.ttl('{}:one_minute'.format(self.r.namespace))
        self.assertAlmostEqual(minute_in_seconds, actual_ttl, delta=4)

    def test_iter(self):
        """Tests the __iter__ function."""
        key_values = {
            'foobar1': 'barbar1',
            'foobar2': 'barbar2',
        }

        for key, value in key_values.items():
            self.r[key] = value

        # TODO made the assumption that iterating the valkeydict should return keys, like a normal dict
        for key in self.r:
            self.assertEqual(self.r[key], key_values[key])

    # TODO behavior of multi and chain methods should be discussed.
    # TODO python 2 couldn't skip
    # @unittest.skip
    # def test_multi_get_with_key_none(self):
    #     """Tests that multi_get with key None raises TypeError."""
    #     with self.assertRaises(TypeError):
    #         self.r.multi_get(None)

    def test_set_and_get(self):
        self.r['key1'] = 'value1'
        self.assertEqual(self.r['key1'], 'value1')

    def test_set_and_delete(self):
        self.r['key2'] = 'value2'
        del self.r['key2']
        self.assertNotIn('key2', self.r)

    def test_set_and_update(self):
        self.r['key3'] = 'value3'
        self.r.update({'key3': 'new_value3'})
        self.assertEqual(self.r['key3'], 'new_value3')

    def test_clear(self):
        self.r['key4'] = 'value4'
        self.r.clear()
        self.assertEqual(len(self.r), 0)

    def test_set_and_pop(self):
        self.r['key5'] = 'value5'
        popped_value = self.r.pop('key5')
        self.assertEqual(popped_value, 'value5')
        self.assertNotIn('key5', self.r)

    def test_set_and_popitem(self):
        self.r['key6'] = 'value6'
        key, value = self.r.popitem()

        self.assertEqual(key, 'key6')
        self.assertEqual(value, 'value6')
        self.assertNotIn('key6', self.r)

    def test_set_and_get_with_different_types(self):
        data = {
            'key_str': 'string_value',
            'key_int': 42,
            'key_float': 3.14,
            'key_bool': True,
            'key_list': [1, 2, 3],
            'key_dict': {'a': 1, 'b': 2},
            'key_none': None
        }

        for key, value in data.items():
            self.r[key] = value

        for key, expected_value in data.items():
            self.assertEqual(self.r[key], expected_value)

    def test_namespace_isolation(self):
        other_namespace = ValkeyDict(namespace='other_namespace')
        self.r['key7'] = 'value7'
        self.assertNotIn('key7', other_namespace)

        # teardown
        other_namespace.clear()

    def test_namespace_global_expire(self):
        other_namespace = ValkeyDict(namespace='other_namespace', expire=1)
        other_namespace['key'] = 'value'

        self.assertEqual(other_namespace['key'], 'value')
        self.assertIn('key', other_namespace)

        time.sleep(2)
        self.assertNotIn('key', other_namespace)
        self.assertRaises(KeyError, lambda: self.r['key11'])

        # teardown
        other_namespace.clear()

    def test_pipeline(self):
        with self.r.pipeline():
            self.r['key8'] = 'value8'
            self.r['key9'] = 'value9'

        self.assertEqual(self.r['key8'], 'value8')
        self.assertEqual(self.r['key9'], 'value9')

    def test_expire_at(self):
        self.r['key10'] = 'value10'
        with self.r.expire_at(1):
            self.r['key11'] = 'value11'

        time.sleep(2)
        self.assertEqual(self.r['key10'], 'value10')
        self.assertRaises(KeyError, lambda: self.r['key11'])

    def test_expire_ttl(self):
        expected = 2
        key, value = 'key12', 'value12'
        self.r[key] = value
        with self.r.expire_at(expected):
            self.r[key] = value
            # test within the context manager
            result = self.r.get_ttl(key)
            self.assertAlmostEqual(expected, result, delta=1)

        # test outside the context manager
        result = self.r.get_ttl(key)

        self.assertAlmostEqual(expected, result, delta=1)
        self.assertEqual(self.r[key], value)

        time.sleep(2.2)
        self.assertRaises(KeyError, lambda: self.r[key])

        # test after expire
        expected = None
        result = self.r.get_ttl(key)
        self.assertEqual(result, expected)

    def test_set_get_empty_tuple(self):
        key = "empty_tuple"
        value = ()
        self.r[key] = value
        self.assertEqual(self.r[key], value)

    def test_set_get_single_element_tuple(self):
        key = "single_element_tuple"
        value = (42,)
        self.r[key] = value
        self.assertEqual(self.r[key], value)

    def test_set_get_empty_set(self):
        key = "empty_set"
        value = set()
        self.r[key] = value
        self.assertEqual(self.r[key], value)

    def test_set_get_single_element_set(self):
        key = "single_element_set"
        value = {42}
        self.r[key] = value
        self.assertEqual(self.r[key], value)

    def test_set_get_mixed_type_list(self):
        key = "mixed_type_list"
        value = [1, "foobar", 3.14, [1, 2, 3]]
        self.r[key] = value
        self.assertEqual(self.r[key], value)

    def test_set_get_mixed_type_list_readme(self):
        key = "mixed_type_list"
        now = datetime.now()
        value = [1, "foobar", 3.14, [1, 2, 3], now]
        self.r[key] = value
        self.assertEqual(self.r[key], value)

    def test_set_get_dict_with_timedelta_readme(self):
        key = "dic_with_timedelta"
        value = {"elapsed_time": timedelta(hours=60)}
        self.r[key] = value
        self.assertEqual(self.r[key], value)

    def test_json_encoder_decoder_readme(self):
        """Test the custom JSON encoder and decoder"""
        now = datetime.now()
        expected = [1, "foobar", 3.14, [1, 2, 3], now]

        encoded = json.dumps(expected, cls=ValkeyDictJSONEncoder)
        result = json.loads(encoded, cls=ValkeyDictJSONDecoder)

        self.assertEqual(result, expected)

    @unittest.skip
    def test_set_get_mixed_type_set(self):
        key = "mixed_type_set"
        value = {1, "foobar", 3.14, (1, 2, 3)}
        self.r[key] = value
        self.assertEqual(self.r[key], value)

    @unittest.skip  # this highlights that sets, and tuples not fully supported
    def test_set_get_nested_tuple(self):
        key = "nested_tuple"
        value = (1, (2, 3), (4, 5))
        self.r[key] = value
        self.assertEqual(self.r[key], value)

    @unittest.skip  # this highlights that sets, and tuples not fully supported
    def test_set_get_nested_tuple_triple(self):
        key = "nested_tuple"
        value = (1, (2, 3), (4, (5, 6)))
        self.r[key] = value
        self.assertEqual(self.r[key], value)

    def test_init_valkey_dict_with_valkey_instance(self):
        test_key = "test_key"
        expected = "expected value"
        test_inputs = {
            "config from_url": redis.Redis.from_url("redis://127.0.0.1/0"),
            "config from kwargs": redis.Redis(**valkey_config),
            "config passed as keywords": redis.Redis(host="127.0.0.1", port=6379),
        }
        for test_name, test_input in test_inputs.items():
            assert_fail_msg = f"test with: {test_name} failed"

            dict_ = ValkeyDict(valkey=test_input)
            dict_[test_key] = expected
            result = dict_[test_key]
            self.assertEqual(result, expected, msg=assert_fail_msg)

            self.assertIs(dict_.valkey, test_input)
            self.assertTrue(
                dict_.valkey.get_connection_kwargs().get("decode_responses"),
                msg=assert_fail_msg,
                )
            test_input.flushdb()


class TestPythonValkeyDict(TestValkeyDict):
    @classmethod
    def create_valkey_dict(cls, namespace=TEST_NAMESPACE_PREFIX, **kwargs):
        config = valkey_config.copy()
        config.update(kwargs)
        return PythonValkeyDict(namespace=namespace+"_PythonValkeyDict", **config)

    @classmethod
    def clear_test_namespace(cls):
        cls.valkeydb.flushdb()
        cls.valkeydb.delete(f"valkey-dict-insertion-order-{TEST_NAMESPACE_PREFIX}")
        for key in cls.valkeydb.scan_iter('{}:*'.format(TEST_NAMESPACE_PREFIX)):
            cls.valkeydb.delete(key)


class TestValkeyDictSecurity(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.valkeydb = redis.StrictRedis(**valkey_config)
        cls.r = cls.create_valkey_dict()

    @classmethod
    def tearDownClass(cls):
        cls.clear_test_namespace()

    @classmethod
    def create_valkey_dict(cls, namespace=TEST_NAMESPACE_PREFIX, **kwargs):
        config = valkey_config.copy()
        config.update(kwargs)
        return ValkeyDict(namespace=namespace, **config)

    @classmethod
    def clear_test_namespace(cls):
        for key in cls.valkeydb.scan_iter('{}:*'.format(TEST_NAMESPACE_PREFIX)):
            cls.valkeydb.delete(key)

    def setUp(self):
        self.clear_test_namespace()

    def _is_python_valkey_dict(self, valkey_dic):
        return getattr(valkey_dic, '_insertion_order_key', None) is not None

    def test_unicode_key(self):
        # Test handling of Unicode keys
        unicode_key = '你好'
        self.r[unicode_key] = 'value'
        self.assertEqual(self.r[unicode_key], 'value')

    def test_unicode_value(self):
        # Test handling of Unicode values
        unicode_value = '世界'
        self.r['key'] = unicode_value
        self.assertEqual(self.r['key'], unicode_value)

    def test_special_characters_key(self):
        special_chars_key = '!@#$%^&*()-=_+[]{}|;:\'",.<>/?`~'
        self.r[special_chars_key] = 'value'
        self.assertEqual(self.r[special_chars_key], 'value')

    def test_special_characters_value(self):
        special_chars_value = '!@#$%^&*()-=_+[]{}|;:\'",.<>/?`~'
        self.r['key'] = special_chars_value
        self.assertEqual(self.r['key'], special_chars_value)

    def test_large_key(self):
        # Test handling of large keys (size limit is 512MB)
        large_key = 'k' * (512 * 1024 * 1024)
        with self.assertRaises(ValueError):
            self.r[large_key] = 'value'

    def test_large_value(self):
        # Test handling of large values (size limit is 512MB)
        large_value = 'v' * (512 * 1024 * 1024)
        with self.assertRaises(ValueError):
            self.r['key'] = large_value

    def test_injection_attack_get(self):
        injection_key = 'key; GET another_key'
        self.r['another_key'] = 'value'
        with self.assertRaises(KeyError):
            self.r[injection_key]
        self.assertEqual(self.r['another_key'], 'value')

        self.r[injection_key] = "foo"
        self.assertEqual(self.r[injection_key], "foo")
        self.assertEqual(self.r['another_key'], 'value')

    def test_injection_attack_mget(self):
        injection_key = 'foo; MGET foo2 foo3'
        self.r['foo2'] = 'bar2'
        self.r['foo3'] = 'bar3'
        with self.assertRaises(KeyError):
            self.r[injection_key]

        if self._is_python_valkey_dict(self.r):
            return
        self.assertEqual(sorted(self.r.multi_get('foo')), sorted(['bar2', 'bar3']))
        self.assertEqual(self.r['foo2'], 'bar2')
        self.assertEqual(self.r['foo3'], 'bar3')

        self.r[injection_key] = "bar"
        if self._is_python_valkey_dict(self.r):
            return
        self.assertEqual(sorted(self.r.multi_get('foo')), sorted(['bar2', 'bar3', 'bar']))
        self.assertEqual(self.r[injection_key], 'bar')
        self.assertEqual(self.r['foo2'], 'bar2')
        self.assertEqual(self.r['foo3'], 'bar3')

    def test_injection_attack_scan(self):
        injection_key = 'bar; SCAN 0 MATCH *'
        self.r['foo2'] = 'bar2'
        self.r['foo3'] = 'bar3'
        with self.assertRaises(KeyError):
            self.r[injection_key]
        self.assertNotIn(injection_key, self.r.keys())
        self.assertEqual(self.r['foo2'], 'bar2')
        self.assertEqual(self.r['foo3'], 'bar3')

        self.r[injection_key] = 'bar'
        self.assertEqual(self.r[injection_key], 'bar')
        self.assertEqual(self.r['foo2'], 'bar2')
        self.assertEqual(self.r['foo3'], 'bar3')

    def test_injection_attack_rename(self):
        injection_key = 'key1; RENAME key2 key3'
        self.r['foo2'] = 'bar2'
        self.r['foo3'] = 'bar3'
        with self.assertRaises(KeyError):
            self.r[injection_key]
        self.assertNotIn(injection_key, self.r.keys())
        self.assertEqual(self.r['foo2'], 'bar2')
        self.assertEqual(self.r['foo3'], 'bar3')

        self.r[injection_key] = 'bar'
        self.assertEqual(self.r[injection_key], 'bar')
        self.assertEqual(self.r['foo2'], 'bar2')
        self.assertEqual(self.r['foo3'], 'bar3')


class TestPythonValkeyDictSecurity(TestValkeyDictSecurity):
    @classmethod
    def create_valkey_dict(cls, namespace=TEST_NAMESPACE_PREFIX, **kwargs):
        config = valkey_config.copy()
        config.update(kwargs)
        return PythonValkeyDict(namespace=namespace+"_PythonValkeyDict", **config)

    @classmethod
    def clear_test_namespace(cls):
        cls.valkeydb.flushdb()
        cls.valkeydb.delete(f"valkey-dict-insertion-order-{TEST_NAMESPACE_PREFIX}")
        for key in cls.valkeydb.scan_iter('{}:*'.format(TEST_NAMESPACE_PREFIX)):
            cls.valkeydb.delete(key)


class TestValkeyDictComparison(unittest.TestCase):
    def setUp(self):
        self.r1 = ValkeyDict(namespace="test1")
        self.r2 = ValkeyDict(namespace="test2")
        self.r3 = ValkeyDict(namespace="test3")
        self.r4 = ValkeyDict(namespace="test4")

        self.r1.update({"a": 1, "b": 2, "c": "foo", "d": [1, 2, 3], "e": {"a": 1, "b": [4, 5, 6]}})
        self.r2.update({"a": 1, "b": 2, "c": "foo", "d": [1, 2, 3], "e": {"a": 1, "b": [4, 5, 6]}})
        self.r3.update({"a": 1, "b": 3, "c": "foo", "d": [1, 2, 3], "e": {"a": 1, "b": [4, 5, 6]}})
        self.r4.update({"a": 1, "b": 2, "c": "foo", "d": [1, 2, 3], "e": {"a": 1, "b": [4, 5, 7]}})

        self.d1 = {"a": 1, "b": 2, "c": "foo", "d": [1, 2, 3], "e": {"a": 1, "b": [4, 5, 6]}}
        self.d2 = {"a": 1, "b": 3, "c": "foo", "d": [1, 2, 3], "e": {"a": 1, "b": [4, 5, 6]}}

    def tearDown(self):
        self.r1.clear()
        self.r2.clear()
        self.r3.clear()
        self.r4.clear()

    @classmethod
    def clear_test_namespace(cls):
        names_spaces = [
            "test1", "test2", "test3", "test4",
            "sequential_comparison", "test_empty",
            "test_nested_empty"
        ]
        for namespace in names_spaces:
            ValkeyDict(namespace).clear()

    @classmethod
    def tearDownClass(cls):
        cls.clear_test_namespace()

    def test_eq(self):
        self.assertTrue(self.r1 == self.r2)
        self.assertFalse(self.r1 == self.r3)
        self.assertFalse(self.r1 == self.r4)
        self.assertTrue(self.r2 == self.r1)
        self.assertFalse(self.r2 == self.r3)
        self.assertFalse(self.r2 == self.r4)
        self.assertFalse(self.r3 == self.r4)

    def test_eq_with_valkey_dict(self):
        self.assertEqual(self.r1, self.r2)

    def test_eq_with_dict(self):
        self.assertEqual(self.r1, self.d1)

    def test_eq_empty(self):
        empty_r = ValkeyDict(namespace="test_empty")
        self.assertEqual(empty_r, {})
        empty_r.clear()

    def test_eq_nested_empty(self):
        nested_empty_r = ValkeyDict(namespace="test_nested_empty")
        nested_empty_r.update({"a": {}})
        nested_empty_d = {"a": {}}
        self.assertEqual(nested_empty_r, nested_empty_d)
        nested_empty_r.clear()

    def test_neq(self):
        self.assertFalse(self.r1 != self.r2)
        self.assertTrue(self.r1 != self.r3)
        self.assertTrue(self.r1 != self.r4)
        self.assertFalse(self.r2 != self.r1)
        self.assertTrue(self.r2 != self.r3)
        self.assertTrue(self.r2 != self.r4)
        self.assertTrue(self.r3 != self.r4)

    def test_neq_with_valkey_dict(self):
        self.assertNotEqual(self.r1, self.r3)
        self.assertNotEqual(self.r1, self.r4)

    def test_neq_with_dict(self):
        self.assertNotEqual(self.r1, self.d2)

    def test_neq_empty(self):
        empty_r = ValkeyDict(namespace="test_empty")
        self.assertNotEqual(self.r1, {})
        self.assertNotEqual(empty_r, self.d1)
        empty_r.clear()

    def test_neq_nested_empty(self):
        nested_empty_r = ValkeyDict(namespace="test_nested_empty")
        nested_empty_r.update({"a": {}})
        nested_empty_d = {"a": {}}
        self.assertNotEqual(self.r1, nested_empty_d)
        self.assertNotEqual(nested_empty_r, self.d1)
        nested_empty_r.clear()

    def test_is_comparison(self):
        self.assertTrue(self.r1 is self.r1)
        self.assertFalse(self.r1 is self.r2)
        self.assertTrue(self.r2 is self.r2)
        self.assertFalse(self.r2 is self.r3)

    def test_is_comparison_with_dict(self):
        self.assertFalse(self.r1 is self.d1)
        self.assertFalse(self.d2 is self.d1)

    def test_is_not_comparison(self):
        self.assertFalse(self.r1 is not self.r1)
        self.assertTrue(self.r1 is not self.r2)
        self.assertFalse(self.r2 is not self.r2)
        self.assertTrue(self.r2 is not self.r3)

    def test_is_not_comparison_with_dict(self):
        self.assertTrue(self.r1 is not self.d1)
        self.assertTrue(self.d2 is not self.d1)

    def test_lt(self):
        with self.assertRaises(TypeError):
            self.r1 < self.r2

    def test_lt_with_different_type(self):
        with self.assertRaises(TypeError):
            self.r1 < self.d1

    def test_le(self):
        with self.assertRaises(TypeError):
            self.r1 <= self.r2

    def test_le_with_different_type(self):
        with self.assertRaises(TypeError):
            self.r1 <= self.d1

    def test_ge(self):
        with self.assertRaises(TypeError):
            self.r1 >= self.r2

    def test_ge_with_different_type(self):
        with self.assertRaises(TypeError):
            self.r1 >= self.d1

    def test_gt(self):
        with self.assertRaises(TypeError):
            self.r1 > self.r2

    def test_gt_with_different_type(self):
        with self.assertRaises(TypeError):
            self.r1 > self.d1

    def test_sequential_comparison(self):
        """"""
        d = {}
        d2 = {}
        rd = ValkeyDict(namespace="sequential_comparison")

        # Testing for identity
        self.assertTrue(d is not d2)
        self.assertTrue(d is not rd)

        # Testing for equality
        self.assertTrue(d == d2)
        self.assertTrue(d == rd)
        self.assertTrue(d.items() == d2.items())
        self.assertTrue(list(d.items()) == list(rd.items()))

        d["foo1"] = "bar1"

        # Testing for inequality after modification in 'd'
        self.assertTrue(d != d2)
        self.assertTrue(d != rd)
        self.assertTrue(d.items() != d2.items())
        self.assertTrue(list(d.items()) != list(rd.items()))

        # Modifying 'd2' and 'rd'
        d2["foo1"] = "bar1"
        rd["foo1"] = "bar1"

        # Testing for equality
        self.assertTrue(d == d2)
        self.assertTrue(d == rd)
        self.assertTrue(d.items() == d2.items())
        self.assertTrue(list(d.items()) == list(rd.items()))

        del d["foo1"]

        # Testing for inequality after modification in 'd'
        self.assertTrue(d != d2)
        self.assertTrue(d != rd)
        self.assertTrue(d.items() != d2.items())
        self.assertTrue(list(d.items()) != list(rd.items()))

        # Modifying 'd2' and 'rd'
        del d2["foo1"]
        del rd["foo1"]

        # Testing for equality
        self.assertTrue(d == d2)
        self.assertTrue(d == rd)
        self.assertTrue(d.items() == d2.items())
        self.assertTrue(list(d.items()) == list(rd.items()))

        d.clear()
        d2.clear()
        rd.clear()

        rd.update({"a": {}})
        d.update({"a": {}})
        d2.update({"a": {}})

        # Testing for nested comparison
        self.assertTrue(d == d2)
        self.assertTrue(d == rd)
        self.assertTrue(d.items() == d2.items())
        self.assertTrue(list(d.items()) == list(rd.items()))

        d.clear()

        # Testing for inequality after clear
        self.assertFalse(d == d2)
        self.assertFalse(d == rd)
        self.assertFalse(d.items() == d2.items())
        self.assertFalse(list(d.items()) == list(rd.items()))

        d2.clear()
        rd.clear()

        # Testing for equality after clear
        self.assertTrue(d == d2)
        self.assertTrue(d == rd)
        self.assertTrue(d.items() == d2.items())
        self.assertTrue(list(d.items()) == list(rd.items()))


class TestPythonValkeyDictComparison(TestValkeyDictComparison):
    def setUp(self):
        self.r1 = PythonValkeyDict(namespace="test1")
        self.r2 = PythonValkeyDict(namespace="test2")
        self.r3 = PythonValkeyDict(namespace="test3")
        self.r4 = PythonValkeyDict(namespace="test4")

        self.r1.update({"a": 1, "b": 2, "c": "foo", "d": [1, 2, 3], "e": {"a": 1, "b": [4, 5, 6]}})
        self.r2.update({"a": 1, "b": 2, "c": "foo", "d": [1, 2, 3], "e": {"a": 1, "b": [4, 5, 6]}})
        self.r3.update({"a": 1, "b": 3, "c": "foo", "d": [1, 2, 3], "e": {"a": 1, "b": [4, 5, 6]}})
        self.r4.update({"a": 1, "b": 2, "c": "foo", "d": [1, 2, 3], "e": {"a": 1, "b": [4, 5, 7]}})

        self.d1 = {"a": 1, "b": 2, "c": "foo", "d": [1, 2, 3], "e": {"a": 1, "b": [4, 5, 6]}}
        self.d2 = {"a": 1, "b": 3, "c": "foo", "d": [1, 2, 3], "e": {"a": 1, "b": [4, 5, 6]}}


class TestValkeyDictPreserveExpire(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.valkeydb = redis.StrictRedis(**valkey_config)
        cls.r = cls.create_valkey_dict()

    @classmethod
    def tearDownClass(cls):
        cls.clear_test_namespace()
        pass

    @classmethod
    def create_valkey_dict(cls, namespace=TEST_NAMESPACE_PREFIX, **kwargs):
        config = valkey_config.copy()
        config.update(kwargs)
        return ValkeyDict(namespace=namespace, **config)

    @classmethod
    def clear_test_namespace(cls):
        cls.valkeydb.delete(f"valkey-dict-insertion-order-{TEST_NAMESPACE_PREFIX}")
        for key in cls.valkeydb.scan_iter('{}:*'.format(TEST_NAMESPACE_PREFIX)):
            cls.valkeydb.delete(key)

    def setUp(self):
        self.clear_test_namespace()

    def test_preserve_expiration(self):
        """Test preserve_expiration configuration parameter."""
        valkey_dict = self.create_valkey_dict(expire=3600, preserve_expiration=True)

        key = "foo"
        value = "bar"
        valkey_dict[key] = value

        # Ensure the TTL (time-to-live) of the "foo" key is approximately the global `expire` time.
        actual_ttl = valkey_dict.get_ttl(key)
        self.assertAlmostEqual(3600, actual_ttl, delta=1)

        time_sleeping = 3
        time.sleep(time_sleeping)

        # Override the "foo" value and create a new "bar" key.
        new_key = "bar"
        valkey_dict[key] = "value"
        valkey_dict[new_key] = "value too"

        # Ensure the TTL of the "foo" key has passed 3 seconds.
        actual_ttl_foo = valkey_dict.get_ttl(key)
        self.assertAlmostEqual(3600 - time_sleeping, actual_ttl_foo, delta=1)

        # Ensure the TTL of the "bar" key is also approximately the global `expire` time.
        actual_ttl_bar = valkey_dict.get_ttl(new_key)

        self.assertAlmostEqual(3600, actual_ttl_bar, delta=1)

        # Ensure the difference between the TTLs of "foo" and "bar" is at least 2 seconds.
        self.assertTrue(abs(actual_ttl_foo - actual_ttl_bar) >= 1)

    def test_preserve_expiration_not_used(self):
        """Test preserve_expiration configuration parameter."""
        valkey_dict = self.create_valkey_dict(expire=3600)

        key = "foo"
        value = "bar"
        valkey_dict[key] = value

        # Ensure the TTL (time-to-live) of the "foo" key is approximately the global `expire` time.
        actual_ttl = valkey_dict.get_ttl(key)
        self.assertAlmostEqual(3600, actual_ttl, delta=1)

        time_sleeping = 3
        time.sleep(time_sleeping)

        # Override the "foo" value and create a new "bar" key.
        new_key = "bar"
        valkey_dict[key] = "value"
        valkey_dict[new_key] = "value too"

        # Ensure the TTL of the "foo" key is global expire again.
        actual_ttl_foo = valkey_dict.get_ttl(key)
        self.assertAlmostEqual(3600, actual_ttl_foo, delta=1)

        # Ensure the TTL of the "bar" key is also approximately the global `expire` time.
        actual_ttl_bar = valkey_dict.get_ttl(new_key)

        self.assertAlmostEqual(3600, actual_ttl_bar, delta=1)

        # Ensure the difference between the TTLs of "foo" and "bar" is no more than one second.
        self.assertTrue(abs(actual_ttl_foo - actual_ttl_bar) <= 1)


class TestPythonValkeyDictPreserveExpire(TestValkeyDictPreserveExpire):
    @classmethod
    def create_valkey_dict(cls, namespace=TEST_NAMESPACE_PREFIX, **kwargs):
        config = valkey_config.copy()
        config.update(kwargs)
        return PythonValkeyDict(namespace=namespace+"_PythonValkeyDict", **config)

    @classmethod
    def clear_test_namespace(cls):
        cls.valkeydb.flushdb()
        cls.valkeydb.delete(f"valkey-dict-insertion-order-{TEST_NAMESPACE_PREFIX}")
        for key in cls.valkeydb.scan_iter('{}:*'.format(TEST_NAMESPACE_PREFIX)):
            cls.valkeydb.delete(key)


class TestValkeyDictMulti(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.valkeydb = redis.StrictRedis(**valkey_config)
        cls.r = cls.create_valkey_dict()

    @classmethod
    def tearDownClass(cls):
        cls.clear_test_namespace()

    @classmethod
    def create_valkey_dict(cls, namespace=TEST_NAMESPACE_PREFIX, **kwargs):
        config = valkey_config.copy()
        config.update(kwargs)
        return ValkeyDict(namespace=namespace, **config)

    @classmethod
    def clear_test_namespace(cls):
        for key in cls.valkeydb.scan_iter('{}:*'.format(TEST_NAMESPACE_PREFIX)):
            cls.valkeydb.delete(key)

    def setUp(self):
        self.clear_test_namespace()

    def test_multi_get_empty(self):
        """Tests the multi_get function with no keys set."""
        self.assertEqual(self.r.multi_get('foo'), [])

    def test_multi_get_nonempty(self):
        """Tests the multi_get function with 3 keys set, get 2 of them."""
        self.r['foobar'] = 'barbar'
        self.r['foobaz'] = 'bazbaz'
        self.r['goobar'] = 'borbor'

        expected_result = ['barbar', 'bazbaz']
        self.assertEqual(sorted(self.r.multi_get('foo')), sorted(expected_result))

    def test_multi_get_chain_with_key_none(self):
        """Tests that multi_chain_get with key None raises TypeError."""
        with self.assertRaises(TypeError):
            self.r.multi_chain_get(None)

    def test_multi_chain_get_empty(self):
        """Tests the multi_chain_get function with no keys set."""
        self.assertEqual(self.r.multi_chain_get(['foo']), [])

    def test_multi_chain_get_nonempty(self):
        """Tests the multi_chain_get function with keys set."""
        self.r.chain_set(['foo', 'bar', 'bar'], 'barbar')
        self.r.chain_set(['foo', 'bar', 'baz'], 'bazbaz')
        self.r.chain_set(['foo', 'baz'], 'borbor')

        # valkey.mget seems to sort keys in reverse order here
        expected_result = sorted([u'bazbaz', u'barbar'])
        self.assertEqual(sorted(self.r.multi_chain_get(['foo', 'bar'])), expected_result)

    def test_multi_dict_empty(self):
        """Tests the multi_dict function with no keys set."""
        self.assertEqual(self.r.multi_dict('foo'), {})

    def test_multi_dict_one_key(self):
        """Tests the multi_dict function with 1 key set."""
        self.r['foobar'] = 'barbar'
        expected_dict = {u'foobar': u'barbar'}
        self.assertEqual(self.r.multi_dict('foo'), expected_dict)

    def test_multi_dict_two_keys(self):
        """Tests the multi_dict function with 2 keys set."""
        self.r['foobar'] = 'barbar'
        self.r['foobaz'] = 'bazbaz'
        expected_dict = {u'foobar': u'barbar', u'foobaz': u'bazbaz'}
        self.assertEqual(self.r.multi_dict('foo'), expected_dict)

    def test_multi_dict_complex(self):
        """Tests the multi_dict function by setting 3 keys and matching 2."""

        self.r['foobar'] = 'barbar'
        self.r['foobaz'] = 'bazbaz'
        self.r['goobar'] = 'borbor'
        expected_dict = {u'foobar': u'barbar', u'foobaz': u'bazbaz'}
        self.assertEqual(self.r.multi_dict('foo'), expected_dict)

    def test_multi_del_empty(self):
        """Tests the multi_del function with no keys set."""
        self.assertEqual(self.r.multi_del('foobar'), 0)

    def test_multi_del_one_key(self):
        """Tests the multi_del function with 1 key set."""
        self.r['foobar'] = 'barbar'
        self.assertEqual(self.r.multi_del('foobar'), 1)
        self.assertIsNone(self.valkeydb.get('foobar'))

    def test_multi_del_two_keys(self):
        """Tests the multi_del function with 2 keys set."""
        self.r['foobar'] = 'barbar'
        self.r['foobaz'] = 'bazbaz'
        self.assertEqual(self.r.multi_del('foo'), 2)
        self.assertIsNone(self.valkeydb.get('foobar'))
        self.assertIsNone(self.valkeydb.get('foobaz'))

    def test_multi_del_complex(self):
        """Tests the multi_del function by setting 3 keys and deleting 2."""
        self.r['foobar'] = 'barbar'
        self.r['foobaz'] = 'bazbaz'
        self.r['goobar'] = 'borbor'
        self.assertEqual(self.r.multi_del('foo'), 2)
        self.assertIsNone(self.valkeydb.get('foobar'))
        self.assertIsNone(self.valkeydb.get('foobaz'))
        self.assertEqual(self.r['goobar'], 'borbor')

    def test_chain_set_2(self):
        """Test setting a chain with 2 elements."""
        self.r.chain_set(['foo', 'bar'], 'melons')

        expected_key = '{}:foo:bar'.format(TEST_NAMESPACE_PREFIX)
        self.assertEqual(self.valkeydb.get(expected_key), b'str:melons')

    def test_chain_set_overwrite(self):
        """Test setting a chain with 1 element and then overwriting it."""
        self.r.chain_set(['foo'], 'melons')
        self.r.chain_set(['foo'], 'bananas')

        expected_key = '{}:foo'.format(TEST_NAMESPACE_PREFIX)
        self.assertEqual(self.valkeydb.get(expected_key), b'str:bananas')

    def test_chain_get_1(self):
        """Test setting and getting a chain with 1 element."""
        self.r.chain_set(['foo'], 'melons')

        self.assertEqual(self.r.chain_get(['foo']), 'melons')

    def test_chain_get_empty(self):
        """Test getting a chain that has not been set."""
        with self.assertRaises(KeyError):
            _ = self.r.chain_get(['foo'])

    def test_chain_get_2(self):
        """Test setting and getting a chain with 2 elements."""
        self.r.chain_set(['foo', 'bar'], 'melons')

        self.assertEqual(self.r.chain_get(['foo', 'bar']), 'melons')

    def test_chain_del_1(self):
        """Test setting and deleting a chain with 1 element."""
        self.r.chain_set(['foo'], 'melons')
        self.r.chain_del(['foo'])

        with self.assertRaises(KeyError):
            _ = self.r.chain_get(['foo'])

    def test_chain_del_2(self):
        """Test setting and deleting a chain with 2 elements."""
        self.r.chain_set(['foo', 'bar'], 'melons')
        self.r.chain_del(['foo', 'bar'])

        with self.assertRaises(KeyError):
            _ = self.r.chain_get(['foo', 'bar'])

    def test_chain_set_1(self):
        """Test setting a chain with 1 element."""
        self.r.chain_set(['foo'], 'melons')

        expected_key = '{}:foo'.format(TEST_NAMESPACE_PREFIX)
        self.assertEqual(self.valkeydb.get(expected_key), b'str:melons')

class TestNotImplementedMethods(unittest.TestCase):
    def setUp(self):
        self.valkey_dict = PythonValkeyDict(namespace="test_namespace")  # Adjust constructor as needed

    def test_multi_get_raises_not_implemented(self):
        """Test that multi_get raises NotImplementedError with correct message"""
        with self.assertRaisesRegex(NotImplementedError, "Not part of PythonValkeyDict"):
            self.valkey_dict.multi_get("test_key")

    def test_multi_chain_get_raises_not_implemented(self):
        """Test that multi_chain_get raises NotImplementedError with correct message"""
        with self.assertRaisesRegex(NotImplementedError, "Not part of PythonValkeyDict"):
            self.valkey_dict.multi_chain_get(["key1", "key2"])

    def test_multi_dict_raises_not_implemented(self):
        """Test that multi_dict raises NotImplementedError with correct message"""
        with self.assertRaisesRegex(NotImplementedError, "Not part of PythonValkeyDict"):
            self.valkey_dict.multi_dict("test_key")

    def test_multi_del_raises_not_implemented(self):
        """Test that multi_del raises NotImplementedError with correct message"""
        with self.assertRaisesRegex(NotImplementedError, "Not part of PythonValkeyDict"):
            self.valkey_dict.multi_del("test_key")


if __name__ == '__main__':
    unittest.main()