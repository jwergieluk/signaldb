#!/bin/env python3

import os
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
    def check_instrument(instrument, ticker_name):
        return True

    @staticmethod
    def extract_series(t, series, series_ref):
        points = []
        for name in series.keys():
            point = OrderedDict()
            point['t'] = t
            point['k'] = series_ref[name]
            point['v'] = series[name]
            points.append(point)
        return points

    def try_save_instrument(self, instrument, ticker_name, collection_name):
        if not self.check_instrument(instrument, ticker_name):
            return False
        if collection_name not in self.db.collection_names():
            self.db.create_collection(collection_name)

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
                return False
        else:
            series_ref = OrderedDict(sorted(static_record["series"].items(), key=lambda k: k[0]))

        series_observations = SignalDb.extract_series(instrument['t'], instrument['series'], series_ref)
        try:
            result = self.db['series'].insert_many(series_observations)
        except pymongo.errors.BulkWriteError:
            return False
        return len(result.inserted_ids) == len(series_observations)



