#!/bin/env python3

import logging
import pymongo
import pymongo.errors
from bson.objectid import ObjectId
from collections import OrderedDict
import pandas


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

    def props(self):
        return self.db[self.properties_col]

    @staticmethod
    def __check_instrument(instrument):
        if not all([k in instrument.keys() for k in ['tickers', 'properties', 'series']]):
            return False
        if len(instrument['tickers']) == 0:
            return False
        return True

    def list_dangling_series(self):
        pass

    def remove_dangling_series(self):
        pass

    def upsert(self, instrument):
        if not self.__check_instrument(instrument):
            self.logger.error("upsert: supplied instrument has wrong type.")
            return False

        main_ticker = None
        first_ticker = instrument['tickers'][0]
        for ticker in instrument['tickers']:
            ticker_record = self.db[self.tickers_col].find_one({'source': ticker[0], 'ticker': ticker[1]})
            if ticker_record is not None:
                main_ticker = ticker_record
                break

        flat_series = []
        if main_ticker is None:
            self.logger.debug("Add new instrument with ticker (%s,%s)" % (first_ticker[0], first_ticker[1]))
            instrument_id = ObjectId()
            instrument['tickers'] = [{'source': ticker[0], 'ticker': ticker[1], 'instr_id': instrument_id}
                                     for ticker in instrument['tickers']]
            self.db[self.tickers_col].insert(instrument['tickers'])

            instrument['properties']['_id'] = instrument_id
            instrument['properties']['series'] = {series_key: ObjectId() for series_key in instrument['series'].keys()}
            self.db[self.properties_col].insert_one(instrument['properties'])

            for key in instrument['series'].keys():
                series = instrument['series'][key]
                series_id = instrument['properties']['series'][key]
                for sample in series:
                    flat_series.append({'k': series_id, 't': sample[0], 'v': sample[1]})
        else:
            instrument_id = main_ticker['instr_id']
            instrument_from_db = self.db[self.properties_col].find_one({'_id': instrument_id})
            if instrument_from_db is None:
                self.logger.warning("The ticker (%s,%s) points to a non-existent properties document." %
                                    (main_ticker['source'], main_ticker['ticker']))
                return False
            updated = False
            for key in instrument['series'].keys():
                series = instrument['series'][key]
                series_id = ObjectId()
                if key not in instrument_from_db['series'].keys():
                    updated = True
                    instrument_from_db['series'][key] = series_id
                else:
                    series_id = instrument_from_db['series'][key]
                for sample in series:
                    flat_series.append({'k': series_id, 't': sample[0], 'v': sample[1]})
                if updated:
                    self.db[self.properties_col].replace_one({'_id': instrument_from_db['_id']}, instrument_from_db)
                    self.logger.debug("Updated the properties of (%s,%s)" %
                                      (main_ticker['source'], main_ticker['ticker']))

        if len(flat_series) > 0:
            self.__upsert_series(flat_series)
        return True

    def upsert_series(self, source: str, ticker, series_name: str, series):
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
        try:
            self.db[self.series_col].insert_many(series)
        except pymongo.errors.BulkWriteError:
            for sample in series:
                sample.pop('_id', None)
                self.db[self.series_col].find_one_and_replace(
                    {'k': sample['k'], 't': sample['t']}, sample, upsert=True)

    def find_instruments(self, search_doc):
        cursor = self.db[self.properties_col].find(search_doc, limit=10000)
        instruments = []
        for instrument in cursor:
            ticker_cursor = self.db[self.tickers_col].find({'instr_id': instrument['_id']})
            tickers = [(ticker['source'], ticker['ticker']) for ticker in ticker_cursor]
            instrument['tickers'] = tickers
            instruments.append(instrument)
        return instruments

    def get_properties(self, source: str, ticker: str):
        ticker_record = self.db[self.tickers_col].find_one({'source': source, 'ticker': ticker})
        if ticker_record is None:
            self.logger.info("Ticker (%s,%s) not found. " % (source, ticker))
            return None
        instrument_from_db = self.db[self.properties_col].find_one({'_id': ticker_record['instr_id']})
        if instrument_from_db is None:
            self.logger.warning("The ticker (%s,%s) points to a non-existent properties document." % (source, ticker))
            return None
        return instrument_from_db

    def get_series(self, source: str, ticker: str):
        instrument = self.get_properties(source, ticker)
        if 'series' not in instrument.keys():
            self.logger.error('The instrument (%s,%s) has no series attached.' % (source, ticker))
            return None
        if len(instrument['series']) == 0:
            self.logger.warn('Instrument %s has no series attached.' % ticker_full_name)
            return None

        series = {}
        for ref in instrument['series'].items():
            times = []
            values = []
            cursor = self.db[self.series_col].find({'k': ref[1]})
            for item in cursor:
                times.append(item['t'])
                values.append(item['v'])

            series[ref[0]] = {'t': times, 'v': values}
        return series

    def get_pandas(self, source: str, ticker: str):
        series = self.get_series(source, ticker)
        if series is None:
            return None
        list_of_pandas = []
        for key in series.keys():
            list_of_pandas.append(pandas.Series(series[key]['v'], index=series[key]['t'], name=key))
        return pandas.concat(list_of_pandas, axis=1)

    @staticmethod
    def __validate_series(series):
        return True
