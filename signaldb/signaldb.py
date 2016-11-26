#!/bin/env python3

import os
import pprint
import pymongo
import pymongo.errors
from bson.objectid import ObjectId
from collections import OrderedDict


class SignalDb:
    def __init__(self):
        cred = {"sdbHost": "192.168.0.16", "sdbPort": 27017, "sdbUser": "worker", "sdbPwd": ""}
        for key in os.environ.keys() & cred.keys():
            try:
                cred[key] = type(cred[key])(os.environ[key])
            except ValueError:
                print("ERROR: %s: Reading connection info failed: no %s in env." % key)
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
                raise ValueError("Collection %s not found" % collection)

    def try_save_instrument(self, instrument, ticker_name, collection_name):
        if not self.__check_instrument(instrument, ticker_name):
            print("# ERROR: SignalDb.try_save_instrument: %s has wrong type." % ticker_name)
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
                print("# ERROR: SignalDb.try_save_instrument: %s not inserted." % ticker)
                return False
        else:
            series_ref = OrderedDict(sorted(static_record["series"].items(), key=lambda k: k[0]))

        series_observations = SignalDb.__extract_series(instrument['t'], instrument['series'], series_ref)
        for observation in series_observations:
            try:
                self.db['series'].insert_one(observation)
            except pymongo.errors.DuplicateKeyError:
                print("# ERROR: Saving observation failed due to unique key (t,k) constraint.")
                continue
        return True

    def __check_add_series(self, collection_name, instrument_doc, series_name):
        new_series_id = ObjectId()
        if 'series' not in instrument_doc.keys():
            instrument_doc['series'] = dict(series_name=new_series_id)
        elif series_name not in instrument_doc['series']:
            instrument_doc['series'][series_name] = new_series_id
        else:
            return

        instrument_doc['series'] = dict(series_name=new_series_id)
        result = self.db[collection_name].update_one({'_id': instrument_doc['_id']},
                                                     {'$set': {'series.' + series_name: new_series_id}})
        if not result.acknowledged:
            return False
        if result.modified_count != 1:
            return False
        return True

    def append_series_to_instrument(self, ticker_name: str, ticker_value, series_name: str, time_series,
                                    collection_name: str):
        # TODO add time_series validation
        series_collection_name = collection_name + ".series"
        self.__check_collections(collection_name, series_collection_name)

        ticker_full_name = "ticker." + ticker_name
        instrument = self.db[collection_name].find_one({ticker_full_name: ticker_value})
        if instrument is None:
            print('# ERROR: Instrument %s not found.' % ticker_name)
            return

        if not self.__check_add_series(collection_name, instrument, series_name):
            print('# ERROR: Unable to update instrument %s.' % ticker_name)
            return False

        decorated_series = self.__decorate_series(time_series, instrument['series'][series_name])
        return False
        if not self.__upload_series(series_collection_name, decorated_series):
            return False
        return True

    @staticmethod
    def __decorate_series(time_series, series_id):
        observations = []
        for p in time_series:
            observations.append(dict(t=p[0], k=series_id, v=p[1]))
        return observations

    def __upload_series(self, series_collection_name, series):
        for observation in series:
            try:
                self.db[series_collection_name].insert_one(observation)
            except pymongo.errors.DuplicateKeyError:
                print('# ERROR: Saving observation failed due to unique key (t,k) constraint.')
                continue
