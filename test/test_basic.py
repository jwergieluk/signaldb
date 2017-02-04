# import os
# import sys
# sys.path.insert(0, os.path.abspath('..'))
import datetime
import random
import unittest
import faker
import logging
import signaldb
from pprint import pprint


class SignalDbTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Setup the connection to the test db and clean it."""
        cls.conn = signaldb.get_db("", "", "", "", "market_test")
        cls.db = signaldb.SignalDb(cls.conn)
        cls.db.purge_db()
        cls.logger = logging.getLogger('')
        cls.logger.setLevel(logging.INFO)

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        super().setUp()

    def tearDown(self):
        super().tearDown()

    def test_upsert_and_check(self):
        """Insert a bunch of instruments, retrieve them back from the db and test if we get the same data"""
        instruments = InstrumentFaker.get_equity(5)
        self.assertTrue(self.db.upsert(instruments))

        for instrument in instruments:
            for ticker in instrument['tickers']:
                with self.subTest(context=ticker):
                    properties_from_db = self.db.get_properties(ticker[0], ticker[1])
                    self.assertIsNotNone(properties_from_db)
                    for property_key in instrument['properties'].keys():
                        self.assertTrue(property_key in properties_from_db)
                        self.assertEqual(instrument['properties'][property_key], properties_from_db[property_key])
                with self.subTest(context=ticker):
                    series_from_db = self.db.get_series(ticker[0], ticker[1])
                    self.assertIsNotNone(series_from_db)
                    series_original = instrument['series']
                    for series_key in series_original.keys():
                        self.assertListEqual(series_from_db[series_key], series_original[series_key])


class InstrumentFaker:
    """Random financial instrument generator"""
    fake = faker.Faker()

    @classmethod
    def get_equity(cls, n=1):
        instruments = []
        for i in range(n):
            instrument = dict(tickers=cls.get_tickers(), properties=cls.get_equity_props(), series={})
            instrument['series'] = {'price': cls.get_series()}
            instruments.append(instrument)
        return instruments

    @classmethod
    def get_equity_props(cls):
        properties = {'category': 'equity',
                      'company_name': cls.fake.company(),
                      'country_code': cls.fake.country_code()}
        return properties

    @classmethod
    def get_tickers(cls):
        tickers = [['ISIN', cls.fake.md5().upper()[:12]], ['BB_CODE', cls.fake.md5().upper()]]
        return tickers

    @classmethod
    def get_series(cls):
        start_date = cls.fake.date_time_between(start_date="-10y", end_date="now", tzinfo=None)
        series = [[start_date + datetime.timedelta(days=i), random.expovariate(1)] for i in range(10)]
        series = [x for x in series if x[0] < datetime.datetime.now()]
        return series




