# import os
# import sys
# sys.path.insert(0, os.path.abspath('..'))
import datetime
import time
import random
import unittest
import faker
import logging
import signaldb
import copy


class SignalDbTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Setup the connection to the test db and clean it."""
        cls.conn = signaldb.get_db("", "", "", "", "market_test")
        cls.db = signaldb.SignalDb(cls.conn)
        cls.db.purge_db()
        cls.logger = logging.getLogger('')
        cls.logger.addHandler(logging.NullHandler())
        cls.instruments_no = 15

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        super().setUp()

    def tearDown(self):
        super().tearDown()

    def test_big_upsert(self):
        InstrumentFaker.time_series_len = 7000
        instruments = InstrumentFaker.get(20)
        self.assertTrue(self.db.upsert(instruments))

    def test_insert_idempotence(self):
        self.db.purge_db()
        instruments = InstrumentFaker.get(self.instruments_no)
        self.assertTrue(self.db.upsert(instruments))
        doc_count = self.db.count_items()
        self.assertTrue(self.db.upsert(instruments))
        self.assertEqual(doc_count, self.db.count_items())

    def test_delete_ticker(self):
        self.db.purge_db()
        now0 = self.db.get_utc_now()
        instruments = InstrumentFaker.get(self.instruments_no)
        self.assertTrue(self.db.upsert(instruments))

        self.assertFalse(self.db.delete('Nonexistent-source', 'Nonexistent-ticker'))
        self.assertFalse(self.db.delete('', 'Nonexistent-ticker'))
        self.assertFalse(self.db.delete('Nonexistent-source', ''))
        # noinspection PyTypeChecker
        self.assertFalse(self.db.delete(0, 0))

        now1 = self.db.get_utc_now()
        time.sleep(0.01)
        source, ticker = instruments[0]['tickers'][0]
        self.assertTrue(self.db.delete(source, ticker))

        time.sleep(0.01)
        self.assertIsNone(self.db.get(source, ticker))
        self.assertIsNone(self.db.get(source, ticker, now0))
        self.assertIsNotNone(self.db.get(source, ticker, now1))

    def test_list_tickers(self):
        self.db.purge_db()
        now0 = self.db.get_utc_now()
        instruments = InstrumentFaker.get(self.instruments_no)
        self.assertTrue(self.db.upsert(instruments))

        # Check if erroneous queries return None
        self.assertIsNone(self.db.list_tickers(0))
        self.assertListEqual(self.db.list_tickers('other_source'), [])

        tickers = [tuple(ticker) for i in instruments for ticker in i['tickers']]
        tickers_from_db = self.db.list_tickers()
        self.assertIsInstance(tickers_from_db, list)
        self.assertSetEqual(set(tickers), set(tickers_from_db))

        tickers = [t for t in tickers if t[0] == 'ISIN']
        tickers_from_db = self.db.list_tickers('ISIN')
        self.assertIsInstance(tickers_from_db, list)
        self.assertSetEqual(set(tickers), set(tickers_from_db))
        # Historical queries
        self.assertListEqual(self.db.list_tickers(now=now0), [])

    def test_get_nonexistent(self):
        instruments = InstrumentFaker.get(self.instruments_no)
        self.assertTrue(self.db.upsert(instruments))

        self.assertIsNone(self.db.get('', 'null_ticker'))
        self.assertIsNone(self.db.get('my_source', ''))
        self.assertIsNone(self.db.get('', ''))
        self.assertIsNone(self.db.get('my_source', 'null_ticker'))

    def test_upsert_unsupported_merge_mode(self):
        instruments = InstrumentFaker.get(1)
        self.assertFalse(self.db.upsert(instruments, props_merge_mode='unsupported'))
        self.assertFalse(self.db.upsert(instruments, series_merge_mode='unsupported'))

    def test_upsert_props_append(self):
        """Test the append mode for updating properties"""
        instruments = InstrumentFaker.get(self.instruments_no)
        self.assertTrue(self.db.upsert(instruments))
        now0 = self.db.get_utc_now()
        instruments0 = copy.deepcopy(instruments)

        for instrument in instruments:
            instrument['properties']['extra_property'] = InstrumentFaker.fake.phone_number()
        self.assertTrue(self.db.upsert(instruments, 'append'))
        self.compare_instruments_with_db(instruments)
        now1 = self.db.get_utc_now()
        instruments1 = copy.deepcopy(instruments)

        instruments_with_props_removed = copy.deepcopy(instruments)
        for instrument in instruments_with_props_removed:
            instrument['properties'].pop('company_name', None)
        self.assertTrue(self.db.upsert(instruments_with_props_removed, 'append'))
        self.compare_instruments_with_db(instruments)
        # History queries
        self.compare_instruments_with_db(instruments0, now0)
        self.compare_instruments_with_db(instruments1, now1)

    def test_upsert_props_replace(self):
        """Test the replace mode for updating properties"""
        instruments = InstrumentFaker.get(self.instruments_no)
        self.assertTrue(self.db.upsert(instruments))
        now0 = self.db.get_utc_now()
        instruments0 = copy.deepcopy(instruments)

        for instrument in instruments:
            instrument['properties']['extra_property'] = InstrumentFaker.fake.phone_number()
            instrument['properties'].pop('company_name', None)
        self.assertTrue(self.db.upsert(instruments, 'replace'))
        self.compare_instruments_with_db(instruments)
        self.compare_instruments_with_db(instruments0, now0)

    def test_upsert_update_series(self):
        """Test adding, modifying and removing series"""
        instruments = InstrumentFaker.get(self.instruments_no)
        self.assertTrue(self.db.upsert(instruments))
        now0 = self.db.get_utc_now()
        instruments0 = copy.deepcopy(instruments)

        for instrument in instruments:
            instrument['series']['new_series'] = InstrumentFaker.get_series()
        self.assertTrue(self.db.upsert(instruments))
        self.compare_instruments_with_db(instruments)
        now1 = self.db.get_utc_now()
        instruments1 = copy.deepcopy(instruments)

        series = instruments[0]['series']['new_series']
        for i, sample in enumerate(series):
            series[i] = [sample[0], 999.9]
        self.assertTrue(self.db.upsert(instruments))
        self.compare_instruments_with_db(instruments)
        now2 = self.db.get_utc_now()
        instruments2 = copy.deepcopy(instruments)

        for instrument in instruments:
            instrument['series'].pop('new_series', None)
        self.assertTrue(self.db.upsert(instruments, series_merge_mode='replace'))
        self.compare_instruments_with_db(instruments)
        now3 = self.db.get_utc_now()
        instruments3 = copy.deepcopy(instruments)

        for instrument in instruments:
            instrument['series']['new_series_2'] = InstrumentFaker.get_series()
        instruments_snapshot = copy.deepcopy(instruments)
        for instrument in instruments:
            instrument['series'].pop('price', None)
        self.assertTrue(self.db.upsert(instruments, series_merge_mode='append'))
        self.compare_instruments_with_db(instruments_snapshot)

        self.compare_instruments_with_db(instruments0, now0)
        self.compare_instruments_with_db(instruments1, now1)
        self.compare_instruments_with_db(instruments2, now2)

    def test_upsert_and_check(self):
        """Insert a bunch of instruments, retrieve them back from the db and test if we've got the same data"""
        instruments = InstrumentFaker.get(self.instruments_no)
        self.assertTrue(self.db.upsert(instruments))
        self.compare_instruments_with_db(instruments)

    def compare_instruments_with_db(self, instruments: list, now=None):
        for instrument in instruments:
            self.compare_instrument_with_db(instrument, now=now)

    def compare_instrument_with_db(self, instrument: dict, now=None):
        for ticker in instrument['tickers']:
            with self.subTest(context=ticker):
                instrument_from_db = self.db.get(ticker[0], ticker[1], now=now)
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
            instrument = {'tickers': cls.get_tickers(), 'properties': cls.get_props(),
                          'series': {'price': cls.get_series(), 'volume': cls.get_series()}}
            instruments.append(instrument)
        return instruments

    @classmethod
    def get_props(cls):
        instrument_type = random.choice(['equity', 'equity_option'])
        if instrument_type == 'equity':
            return cls.get_equity_props()
        if instrument_type == 'equity_option':
            return cls.get_equity_option_props()
        return {}

    @classmethod
    def get_equity_props(cls):
        properties = {'category': 'equity',
                      'company_name': cls.fake.company(),
                      'country_code': cls.fake.country_code()}
        return properties

    @classmethod
    def get_equity_option_props(cls):
        properties = {'category': 'equity-option',
                      'underlying_entity': cls.fake.company(),
                      'strike': random.expovariate(100),
                      'maturity': cls.fake.date_time_this_century(after_now=True),
                      'option_type': random.choice(['put', 'call'])}
        return properties

    @classmethod
    def get_tickers(cls):
        tickers = [['ISIN', cls.fake.md5().upper()[:12]], ['BB_CODE', cls.fake.md5().upper()]]
        return tickers

    @classmethod
    def get_series(cls):
        start_date = datetime.datetime.now().replace(year=2000)
#        start_date = cls.fake.date_time_between(start_date="-10y", end_date="now", tzinfo=None)
        series = [[start_date + datetime.timedelta(days=i), random.expovariate(1)]
                  for i in range(cls.time_series_len)]
        series = [x for x in series if x[0] < datetime.datetime.now()]
        return series




