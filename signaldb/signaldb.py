#!/bin/env python3

import datetime
import logging
import pymongo
import pymongo.errors
import signaldb
from bson.objectid import ObjectId


def merge_props(current_props, new_props, merge_props_mode):
    """Add new properties to a given properties document."""
    current_props_modified = False
    if merge_props_mode == 'append':
        for key in new_props:
            if key not in current_props.keys():
                current_props[key] = new_props[key]
                current_props_modified = True
    return current_props_modified


class SignalDb:
    def __init__(self, db):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self.db = db
        self.properties_col = 'properties'
        self.tickers_col = 'tickers'
        self.series_col = 'series'

        self.db[self.tickers_col].create_index(
            [('source', pymongo.ASCENDING), ('ticker', pymongo.ASCENDING)], unique=True, name='source_ticker_index')
        self.db[self.tickers_col].create_index('instr_id', unique=False, name='instr_id_index')
        self.db[self.series_col].create_index(
            [('k', pymongo.ASCENDING), ('t', pymongo.ASCENDING)], unique=True, name='k_t_index')

    def purge_db(self):
        """Remove all data from the database."""
        self.logger.debug('Removing all data from db.')
        self.db[self.tickers_col].delete_many({})
        self.db[self.series_col].delete_many({})
        self.db[self.properties_col].delete_many({})

    @staticmethod
    def check_instrument(instrument):
        """Check if an instrument object has a valid type."""
        if type(instrument) is not dict:
            return 1
        if not all([k in instrument.keys() for k in ['tickers', 'properties', 'series']]):
            return 2
        if type(instrument['tickers']) is not list:
            return 3
        if len(instrument['tickers']) == 0:
            return 3
        for ticker in instrument['tickers']:
            if type(ticker) not in [list, tuple]:
                return 5
            if len(ticker) != 2:
                return 6
            if not all([type(ticker_part) is str for ticker_part in ticker]):
                return 7
            if not all([len(ticker_part) > 0 for ticker_part in ticker]):
                return 8
        if type(instrument['series']) is not dict:
            return 9
        for series_key in instrument['series']:
            if type(series_key) is not str:
                return 10
            if len(series_key) == 0:
                return 11
            series = instrument['series'][series_key]
            for sample in series:
                if type(sample) not in [list, tuple]:
                    return 12
                if len(sample) != 2:
                    return 13
                if type(sample[0]) not in [datetime.date, datetime.datetime]:
                    return 14
        return 0

    def list_dangling_series(self):
        pass

    def remove_dangling_series(self):
        pass

    def upsert(self, instruments, merge_props_mode='append'):
        """Update or insert a list of instruments."""
        if type(instruments) not in [list, tuple, dict]:
            self.logger.error("upsert: supplied instrument data is not dict, list, or tuple")
            return False
        if type(instruments) is dict:
            instruments = [instruments]
        signaldb.recursive_str_to_datetime(instruments)
        for i, instrument in enumerate(instruments):
            check_result = self.check_instrument(instrument)
            if check_result != 0:
                self.logger.error("upsert: supplied instrument has wrong type (index no %d; failed test %d)." %
                                  (i, check_result))
                continue
            self.__upsert_instrument(instrument, merge_props_mode)
        return True

    def __upsert_instrument(self, instrument, merge_props_mode):
        """Update or insert an instrument"""
        main_ticker = None
        for ticker in instrument['tickers']:
            ticker_record = self.db[self.tickers_col].find_one({'source': ticker[0], 'ticker': ticker[1]})
            if ticker_record is not None:
                main_ticker = ticker_record
                break

        flat_series = []
        first_ticker = instrument['tickers'][0]
        if main_ticker is None:
            self.logger.debug("Add new instrument with ticker (%s,%s)" % (first_ticker[0], first_ticker[1]))
            instrument_id = ObjectId()

            tickers_for_insert = [{'_id': ObjectId(), 'source': ticker[0], 'ticker': ticker[1],
                                   'instr_id': instrument_id} for ticker in instrument['tickers']]
            instrument['properties']['_id'] = instrument_id
            instrument['properties']['series'] = {series_key: ObjectId() for series_key in instrument['series'].keys()}

            for key in instrument['series'].keys():
                series = instrument['series'][key]
                series_id = instrument['properties']['series'][key]
                for sample in series:
                    flat_series.append({'k': series_id, 't': sample[0], 'v': sample[1]})
            try:
                self.db[self.tickers_col].insert(tickers_for_insert)
                self.db[self.properties_col].insert_one(instrument['properties'])
            except KeyboardInterrupt:
                self.db[self.tickers_col].delete_many({'_id': {'$in': [t['_id'] for t in tickers_for_insert]}})
                raise
        else:
            instrument_id = main_ticker['instr_id']
            current_props = self.db[self.properties_col].find_one({'_id': instrument_id})
            if current_props is None:
                self.logger.warning('Repair the dangling ticker (%s,%s)' %
                                    (main_ticker['source'], main_ticker['ticker']))
                self.db[self.tickers_col].delete_one(main_ticker)
                return self.__upsert_instrument(instrument, merge_props_mode)
            updated = merge_props(current_props, instrument['properties'], merge_props_mode)
            for key in instrument['series'].keys():
                series = instrument['series'][key]
                series_id = ObjectId()
                if key not in current_props['series'].keys():
                    updated = True
                    current_props['series'][key] = series_id
                else:
                    series_id = current_props['series'][key]
                for sample in series:
                    flat_series.append({'k': series_id, 't': sample[0], 'v': sample[1]})
            if updated:
                self.db[self.properties_col].replace_one({'_id': current_props['_id']}, current_props)
        self.__upsert_series(flat_series)
        instrument['properties'].pop('series', None)
        return True

    def upsert_series(self, source: str, ticker, series_name: str, series):
        """Upsert a series of an existing instrument."""
        if not self.__validate_series(series):
            self.logger.error("Invalid series for (%s,%s) provided." % (source, ticker))
            return False
        instrument = self.get_properties(source, ticker)
        if instrument is None:
            self.logger.error("(%s,%s) not found." % (source, ticker))
            return False

        series_id = ObjectId()
        if 'series' not in instrument.keys():
            instrument['series'] = dict()
        if series_name not in instrument['series'].keys():
            instrument['series'][series_name] = series_id
            self.db[self.properties_col].replace_one({'_id': instrument['_id']}, instrument)
        else:
            series_id = instrument['series'][series_name]

        series_for_insert = []
        for sample in series:
            series_for_insert.append(dict(k=series_id, t=sample[0], v=sample[1]))

        self.__upsert_series(series_for_insert)
        return True

    def __upsert_series(self, series):
        """Insert a list of observations to the series col. Updates existing observations."""
        if len(series) == 0:
            return
        try:
            self.db[self.series_col].insert_many(series)
        except pymongo.errors.BulkWriteError:
            for sample in series:
                sample.pop('_id', None)
                self.db[self.series_col].find_one_and_replace(
                    {'k': sample['k'], 't': sample['t']}, sample, upsert=True)

    def find_instruments(self, filter_doc):
        cursor = self.db[self.properties_col].find(filter_doc, limit=10000)
        instruments = []
        for instrument in cursor:
            ticker_cursor = self.db[self.tickers_col].find({'instr_id': instrument['_id']})
            tickers = [(ticker['source'], ticker['ticker']) for ticker in ticker_cursor]
            instrument['tickers'] = tickers
            instruments.append(instrument)
        return instruments

    def get(self, source: str, ticker: str):
        """Find a single instrument and return it in the standard form"""
        ticker_record = self.db[self.tickers_col].find_one({'source': source, 'ticker': ticker})
        if ticker_record is None:
            self.logger.info("Ticker (%s,%s) not found. " % (source, ticker))
            return None
        properties = self.db[self.properties_col].find_one({'_id': ticker_record['instr_id']})
        if properties is None:
            self.logger.warning("The ticker (%s,%s) points to a non-existent properties document." % (source, ticker))
            return None
        instrument = dict(properties=properties, tickers=[[source, ticker], ])
        if 'series' not in properties.keys():
            self.logger.warning('The instrument (%s,%s) has no series attached.' % (source, ticker))
            instrument['series'] = {}
            return instrument
        instrument['series'] = properties.pop('series')
        if len(instrument['series']) == 0:
            self.logger.warning('Instrument (%s,%s) has no series attached.' % (source, ticker))
            return instrument
        series = {}
        for ref in instrument['series'].items():
            observations = []
            cursor = self.db[self.series_col].find({'k': ref[1]})
            for item in cursor:
                observations.append([item['t'], item['v']])
            series[ref[0]] = observations
        instrument['series'] = series
        return instrument

    @staticmethod
    def __validate_series(series):
        return True
