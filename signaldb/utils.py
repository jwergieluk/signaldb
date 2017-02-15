import pytz
import rfc3339
import re
import datetime
import json
import logging
import os
import pymongo
from bson.objectid import ObjectId


def str_to_datetime(s):
    d = rfc3339.parse_datetime(s)
    utc_time_zone = pytz.timezone('UTC')
    d = d.astimezone(utc_time_zone).replace(tzinfo=None)
    return d.replace(microsecond=(d.microsecond // 1000)*1000)


def recursive_str_to_datetime(obj):
    """Recursively travels a dict/list tree and replaces every str with datetime if possible"""
    if type(obj) is list:
        for i, e in enumerate(obj):
            if type(e) is str:
                if re.fullmatch('^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})Z$', e) is not None:
                    obj[i] = str_to_datetime(e)
            else:
                recursive_str_to_datetime(e)
    if type(obj) is dict:
        for key in obj.keys():
            if type(obj[key]) is str:
                if re.fullmatch('^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})Z$', obj[key]) is not None:
                    obj[key] = str_to_datetime(obj[key])
            else:
                recursive_str_to_datetime(obj[key])


def truncate_microseconds(d: datetime.datetime):
    return d.replace(microsecond=(d.microsecond // 1000) * 1000)


class JSONEncoderExtension(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return rfc3339.datetimetostr(obj)
        if isinstance(obj, ObjectId):
            return 'ObjectId(%s)' % str(obj)
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


def read_values_from_env(conf: dict):
    status = True
    for key in os.environ.keys() & conf.keys():
        try:
            conf[key] = type(conf[key])(os.environ[key])
        except ValueError:
            logging.getLogger().warning("Failed reading %s from environment." % key)
            status = False
    return status


def get_db(host, port, user, pwd, db_name):
    cred = {"sdb_host": "", "sdb_port": 27017, "sdb_user": "", "sdb_pwd": ""}
    read_values_from_env(cred)

    if len(host) != 0:
        cred['sdb_host'] = host
    if len(str(port)) != 0:
        cred['sdb_port'] = port
    if len(user) != 0:
        cred['sdb_user'] = user
    if len(pwd) != 0:
        cred['sdb_pwd'] = pwd

    if not all([len(str(cred[key])) > 0 for key in cred.keys()]):
        cred['sdb_pwd'] = '*' * len(cred['sdb_pwd'])
        logging.getLogger().error('Connection details missing: ' +
                                  ' '.join(tuple(['%s:%s' % (key, str(cred[key])) for key in cred.keys()])))
        raise SystemExit(1)

    mongo_client = pymongo.MongoClient(cred["sdb_host"], cred["sdb_port"])
    db = mongo_client[db_name]
    db.authenticate(cred["sdb_user"], cred["sdb_pwd"], source='admin')
    return db
