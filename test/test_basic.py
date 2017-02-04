#import os
#import sys
#sys.path.insert(0, os.path.abspath('..'))
import unittest
import signaldb


class SignalDbTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Setup the connection to the test db and clean it."""
        cls.conn = signaldb.get_db("", "", "", "", "market_test")
        cls.db = signaldb.SignalDb(cls.conn)
        cls.db.purge_db()

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        super().setUp()

    def tearDown(self):
        super().tearDown()
        self.db.purge_db()

    def test(self):
        self.assertEqual(1, 1)
