#!/bin/env python3

import os
import logging
import pymongo
import pymongo.errors
from bson.objectid import ObjectId
from collections import OrderedDict
import pandas

class SignalDb:
    def __init__(self):
        self.logger = logging.getLogger('signal.SignalDb')
        self.logger.setLevel(logging.DEBUG)
        cred = {"sdbHost": "192.168.0.16", "sdbPort": 27017, "sdbUser": "worker", "sdbPwd": ""}
        for key in os.environ.keys() & cred.keys():
            try:
                cred[key] = type(cred[key])(os.environ[key])
            except ValueError:
                self.logger.error("Reading connection info failed: no %s in env." % key)
                raise SystemError

        self.mongo_client = pymongo.MongoClient(cred["sdbHost"], cred["sdbPort"])
        self.db = self.mongo_client['market']
        self.db.authenticate(cred["sdbUser"], cred["sdbPwd"], source='admin')

    @staticmethod
    def __check_instrument(instrument, ticker_name):
        if not all([k in instrument.keys() for k in ['t', 'static', 'series']]):
            return False
        if ticker_name not in instrument['static']['ticker']:
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

    def __check_collections(self, *collections):
        for collection in collections:
            if collection not in self.db.collection_names():
                self.logger.error("__check_collections: Collection %s not found" % collection)
                raise ValueError

    def try_save_instrument(self, instrument, ticker_name, collection_name):
        if not self.__check_instrument(instrument, ticker_name):
            self.logger.error("try_save_instrument: %s has wrong type." % ticker_name)
            return False
        series_collection_name = collection_name + ".series"
        self.__check_collections(collection_name, series_collection_name)

        ticker_field = "ticker.%s" % ticker_name
        ticker = instrument["static"]["ticker"][ticker_name]
        static_record = self.db[collection_name].find_one({ticker_field: ticker})
        is_new = static_record is None

        series_ref = OrderedDict(instrument["series"])
        if is_new:
            for key in series_ref.keys():
                series_ref[key] = ObjectId()
            instrument["static"]["series"] = series_ref
            result = self.db[collection_name].insert_one(instrument['static'])
            if result is None:
                self.logger.error("try_save_instrument: %s not inserted." % ticker)
                return False
        else:
            series_ref = OrderedDict(sorted(static_record["series"].items(), key=lambda k: k[0]))

        series = SignalDb.__extract_series(instrument['t'], instrument['series'], series_ref)
        self.__upload_series(series_collection_name, series)
        return True

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
        self.__check_collections(collection_name, series_collection_name)

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
