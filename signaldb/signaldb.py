#!/bin/env python3

import os
import logging
import pymongo
import pymongo.errors
from bson.objectid import ObjectId
from collections import OrderedDict
import pandas


class SignalDb:
    def __init__(self, db):
        self.logger = logging.getLogger('signal.SignalDb')
        self.logger.setLevel(logging.DEBUG)
        self.db = db
        self.properties_col = 'properties'
        self.tickers_col = 'tickers'
        self.series_col = 'series'

        self.db[self.tickers_col].create_index(
            [('source', pymongo.ASCENDING), ('ticker', pymongo.ASCENDING)], unique=True, name='source_ticker_index')
        self.db[self.series_col].create_index(
            [('k', pymongo.ASCENDING), ('t', pymongo.ASCENDING)], unique=True, name='k_t_index')

    @staticmethod
    def __check_instrument(instrument):
        if not all([k in instrument.keys() for k in ['tickers', 'properties', 'series']]):
            return False
        if len(instrument['tickers']) == 0:
            return False
        return True

    @staticmethod
    def __extract_series(t, series, series_ref):
        points = []
        for name in series.keys():
            point = OrderedDict()
            point['t'] = t
            point['k'] = series_ref[name]
            point['v'] = series[name]
            points.append(point)
        return points

    def upsert(self, instrument):
        if not self.__check_instrument(instrument):
            self.logger.error("upsert: supplied instrument has wrong type.")
            return False

        main_ticker = None
        for ticker in instrument['tickers']:
            ticker_record = self.db[self.tickers_col].find_one({'source': ticker[0], 'ticker': ticker[1]})
            if ticker_record is not None:
                main_ticker = ticker_record
                break

        flat_series = []
        if main_ticker is None:
            self.logger.debug("Add new instrument with ticker (%s,%s)" % (ticker[0], ticker[1]))
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
            try:
                self.db[self.series_col].insert_many(flat_series)
            except pymongo.errors.BulkWriteError:
                for sample in flat_series:
                    sample.pop('_id', None)
                    self.db[self.series_col].find_one_and_replace(
                        {'k': sample['k'], 't': sample['t']}, sample, upsert=True)

    def __check_add_series_ref(self, collection_name, instrument_doc, series_name):
        new_series_id = ObjectId()
        if 'series' not in instrument_doc.keys():
            instrument_doc['series'] = dict(series_name=new_series_id)
        elif series_name not in instrument_doc['series'].keys():
            instrument_doc['series'][series_name] = new_series_id
        else:
            return True

        result = self.db[collection_name].update_one({'_id': instrument_doc['_id']},
                                                     {'$set': {'series.' + series_name: new_series_id}})
        if not result.acknowledged:
            return False
        if result.modified_count != 1:
            return False
        return True

    def append_series_to_instrument(self, ticker_provider: str, ticker, series_name: str, series,
                                    collection_name: str):
        # TODO add time_series validation
        series_collection_name = collection_name + ".series"
#        self.__check_collections(collection_name, series_collection_name)

        ticker_full_name = "ticker." + ticker_provider
        instrument = self.db[collection_name].find_one({ticker_full_name: ticker})
        if instrument is None:
            self.logger.error('Instrument %s not found.' % ticker)
            return False

        if not self.__check_add_series_ref(collection_name, instrument, series_name):
            self.logger.error('Unable to update instrument %s.' % ticker)
            return False

        decorated_series = self.__decorate_series(series, instrument['series'][series_name])
        if not self.__upload_series(series_collection_name, decorated_series):
            return False
        return True

    @staticmethod
    def __decorate_series(time_series, series_id: ObjectId):
        observations = []
        for p in time_series:
            observations.append(OrderedDict(t=p[0], k=series_id, v=p[1]))
        return observations

    def __upload_series(self, collection_name: str, series):
        duplicates_no = 0
        for observation in series:
            try:
                self.db[collection_name].insert_one(observation)
            except pymongo.errors.DuplicateKeyError:
                duplicates_no += 1
                continue
        if duplicates_no > 0:
            self.logger.warn('%d duplicate observations discarded (out of %d).' % (duplicates_no, len(series)))

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

    def get_series_new(self, source: str, ticker: str):
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
#            series.append(pandas.Series(values, index=times, name=ref[0]))
#        return pandas.concat(series, axis=1)

    def __get_multiple_series(self, collection_name, series_refs):
        series = []
        for ref in series_refs:
            times = []
            values = []
            cursor = self.db[collection_name].find({'k': ref[1]})
            for item in cursor:
                times.append(item['t'])
                values.append(item['v'])
            series.append(pandas.Series(values, index=times, name=ref[0]))
        return pandas.concat(series, axis=1)

    def get_series(self, collection_name: str, ticker_provider: str, ticker: str, series_name: str = ""):
        series_collection_name = collection_name + ".series"
        self.__check_collections(collection_name, series_collection_name)

        ticker_full_name = "ticker." + ticker_provider
        instrument = self.db[collection_name].find_one({ticker_full_name: ticker})
        if instrument is None:
            self.logger.error('Instrument %s not found.' % ticker)
            return None

        if 'series' not in instrument.keys():
            self.logger.error('Instrument %s has no series attached.' % ticker_full_name)
            return None

        if len(series_name) == 0:
            if len(instrument['series']) == 0:
                self.logger.warn('Instrument %s has no series attached.' % ticker_full_name)
                return None
            return self.__get_multiple_series(series_collection_name, instrument['series'].items())

        if series_name not in instrument['series'].keys():
            self.logger.error('Instrument %s has no series %s attached.' % (ticker_full_name, series_name))
            return None

        return self.__get_multiple_series(series_collection_name, [(series_name, instrument['series'][series_name])])



