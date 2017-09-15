import signaldb
import datetime
import unittest


class TestRfc3339(unittest.TestCase):
    def test_basic1(self):
        d1 = datetime.datetime.utcnow()
        s = signaldb.rfc3339_datetime_to_str(d1)
        d2 = signaldb.rfc3339_str_to_datetime(s)
        self.assertEqual(d1, d2)

    def test_basic2(self):
        s1 = '2017-09-15T21:19:14.696008Z'
        d = signaldb.rfc3339_str_to_datetime(s1)
        s2 = signaldb.rfc3339_datetime_to_str(d)
        self.assertEqual(s1, s2)
