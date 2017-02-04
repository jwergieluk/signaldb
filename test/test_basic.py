# import os
# import sys
# sys.path.insert(0, os.path.abspath('..'))
import datetime
import random
import unittest
import faker
import logging
import signaldb
import copy
from pprint import pprint


class SignalDbTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Setup the connection to the test db and clean it."""
        cls.conn = signaldb.get_db("", "", "", "", "market_test")
        cls.db = signaldb.SignalDb(cls.conn)
        cls.db.purge_db()
        cls.logger = logging.getLogger('')
        cls.logger.addHandler(logging.NullHandler())
        cls.instruments_no = 4

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        super().setUp()

    def tearDown(self):
        super().tearDown()

    def test_get_nonexistent(self):
        instruments = InstrumentFaker.get(self.instruments_no)
        self.assertTrue(self.db.upsert(instruments))

        self.assertIsNone(self.db.get('', 'null_ticker'))
        self.assertIsNone(self.db.get('my_source', ''))
        self.assertIsNone(self.db.get('', ''))
        self.assertIsNone(self.db.get('my_source', 'null_ticker'))

    def test_upsert_unsupported_merge_mode(self):
        instruments = InstrumentFaker.get(1)
        self.assertFalse(self.db.upsert(instruments, merge_props_mode='unsupported'))

    def test_upsert_props_append(self):
        """Test if updating existing instruments works as expected"""
        instruments = InstrumentFaker.get(self.instruments_no)
        self.assertTrue(self.db.upsert(instruments))

        for instrument in instruments:
            instrument['properties']['extra_property'] = InstrumentFaker.fake.phone_number()
        self.assertTrue(self.db.upsert(instruments, 'append'))
        for instrument in instruments:
            self.compare_instrument_with_db(instrument)

        instruments_with_props_removed = copy.deepcopy(instruments)
        for instrument in instruments_with_props_removed:
            instrument['properties'].pop('company_name', None)
        self.assertTrue(self.db.upsert(instruments_with_props_removed, 'append'))
        for instrument in instruments:
            self.compare_instrument_with_db(instrument)

    def test_upsert_update_series(self):
        """Test adding, modifying and removing series"""
        instruments = InstrumentFaker.get(self.instruments_no)
        self.assertTrue(self.db.upsert(instruments))

        for instrument in instruments:
            instrument['series']['new_series'] = InstrumentFaker.get_series()
        self.assertTrue(self.db.upsert(instruments))
        for instrument in instruments:
            self.compare_instrument_with_db(instrument)

        series = instruments[0]['series']['new_series']
        for i, sample in enumerate(series):
            series[i] = [sample[0], random.normalvariate(0, 1)]
        self.assertTrue(self.db.upsert(instruments))
        for instrument in instruments:
            self.compare_instrument_with_db(instrument)

        for instrument in instruments:
            instrument['series'].pop('new_series', None)
        self.assertTrue(self.db.upsert(instruments))
        for instrument in instruments:
            self.compare_instrument_with_db(instrument)

    def test_upsert_and_check(self):
        """Insert a bunch of instruments, retrieve them back from the db and test if we've got the same data"""
        instruments = InstrumentFaker.get(self.instruments_no)
        self.assertTrue(self.db.upsert(instruments))

        for instrument in instruments:
            self.compare_instrument_with_db(instrument)

    def compare_instrument_with_db(self, instrument):
        for ticker in instrument['tickers']:
            with self.subTest(context=ticker):
                instrument_from_db = self.db.get(ticker[0], ticker[1])
                self.assertEqual(self.db.check_instrument(instrument_from_db), 0)

                """Test properties"""
                properties_from_db = instrument_from_db['properties']
                properties_from_db.pop('_id', None)
                self.assertDictEqual(properties_from_db, instrument['properties'])
                for property_key in instrument['properties'].keys():
                    self.assertTrue(property_key in properties_from_db)
                    self.assertEqual(instrument['properties'][property_key], properties_from_db[property_key])

                """Test series"""
                series_from_db = instrument_from_db['series']
                series_original = instrument['series']
                self.assertSetEqual(set(series_original.keys()), set(series_from_db.keys()))
                for series_key in series_original.keys():
                    self.assertListEqual(series_from_db[series_key], series_original[series_key])


class InstrumentFaker:
    """Random financial instrument generator"""
    fake = faker.Faker()
    time_series_len = 10

    @classmethod
    def get(cls, n=1):
        instruments = []
        for i in range(n):
            instrument = {'tickers': cls.get_tickers(), 'properties': cls.get_equity_props(),
                          'series': {'price': cls.get_series()}}
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
        series = [[start_date + datetime.timedelta(days=i), random.expovariate(1)]
                  for i in range(cls.time_series_len)]
        series = [x for x in series if x[0] < datetime.datetime.now()]
        return series




