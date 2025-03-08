import time
import unittest

import redis
from valkey_dict import ValkeyDict

from datetime import timedelta

TEST_NAMESPACE_PREFIX = '__test_prefix_key_meta_8fee__'

valkey_config = {
    'host': 'localhost',
    'port': 6379,
    'db': 11,
}


class TestValkeyDictExpire(unittest.TestCase):
    TEST_NAMESPACE_PREFIX = TEST_NAMESPACE_PREFIX + "_DictCompliant"

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
        return ValkeyDict(namespace=namespace+"_DictCompliant", **config)

    @classmethod
    def clear_test_namespace(cls):
        for key in cls.valkeydb.scan_iter('{}:*'.format(TEST_NAMESPACE_PREFIX)):
            cls.valkeydb.delete(key)

    def setUp(self):
        self.clear_test_namespace()


    def test_expire_keyword(self):
        """Test adding keys with an `expire` value by using the `expire` config keyword."""
        expected = 3600
        key = 'foobar1'
        r = self.create_valkey_dict(expire=expected)
        self.assertEqual(expected, r.expire)
        r[key] = 'barbar'

        actual_ttl = self.r.valkey.ttl(f'{self.r.namespace}:{key}')
        self.assertAlmostEqual(expected, actual_ttl, delta=10)


    def test_preserve_expiration(self):
        """Test preserve_expiration configuration parameter."""
        expected = 3600
        valkey_dict = self.create_valkey_dict(expire=expected, preserve_expiration=True)

        key = "foo"
        value = "bar"
        valkey_dict[key] = value

        # Ensure the TTL (time-to-live) of the "foo" key is approximately the global `expire` time.
        actual_ttl = valkey_dict.get_ttl(key)
        self.assertEqual(expected, valkey_dict.expire)
        print(expected, valkey_dict.expire, actual_ttl)
        self.assertAlmostEqual(expected, actual_ttl, delta=1)

        time_sleeping = 3
        time.sleep(time_sleeping)

        # Override the "foo" value and create a new "bar" key.
        new_key = "bar"
        valkey_dict[key] = "value"
        valkey_dict[new_key] = "value too"

        # Ensure the TTL of the "foo" key has passed 3 seconds.
        actual_ttl_foo = valkey_dict.get_ttl(key)
        self.assertAlmostEqual(expected - time_sleeping, actual_ttl_foo, delta=1)

        # Ensure the TTL of the "bar" key is also approximately the global `expire` time.
        actual_ttl_bar = valkey_dict.get_ttl(new_key)

        self.assertAlmostEqual(expected, actual_ttl_bar, delta=1)

        # Ensure the difference between the TTLs of "foo" and "bar" is at least 2 seconds.
        self.assertTrue(abs(actual_ttl_foo - actual_ttl_bar) >= 1)

    @unittest.skip
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

    @unittest.skip
    def test_expire_context(self):
        """Test adding keys with an `expire` value by using the contextmanager."""
        with self.r.expire_at(3600):
            self.r['foobar'] = 'barbar'

        actual_ttl = self.valkeydb.ttl('{}:foobar'.format(self.r.namespace))
        self.assertAlmostEqual(3600, actual_ttl, delta=2)

if __name__ == '__main__':
    unittest.main()
