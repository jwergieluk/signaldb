import pytz
import rfc3339
import re
import datetime
import time
import json
import logging
import pymongo
from bson.objectid import ObjectId
import os


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


def get_utc_now():
    now = datetime.datetime.utcnow().replace(tzinfo=None)
    return now.replace(microsecond=(now.microsecond // 1000)*1000)


class JSONEncoderExtension(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return rfc3339.datetimetostr(obj)
        if isinstance(obj, ObjectId):
            return 'ObjectId(%s)' % str(obj)
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


def get_mondodb_conn_from_env():
    host = os.environ['mongodb_host']
    port = os.environ['mongodb_port']
    user = ''
    if 'mongodb_user' in os.environ.keys():
        user = os.environ['mongodb_user']
    pwd = ''
    if 'mongodb_pwd' in os.environ.keys():
        pwd = os.environ['mongodb_pwd']
    col = ''
    if 'signaldb_collection' in os.environ.keys():
        col = os.environ['signaldb_collection']
    return get_mongodb_conn(host, port, user, pwd, col)


def get_mongodb_conn(host, port, user, pwd, collection_name):
    time_stamp = time.perf_counter()

    if len(host) == 0:
        logging.getLogger(__name__).error('Missing host name')
        return None
    if len(port) == 0:
        logging.getLogger(__name__).error('Missing port number')
        return None
    if len(collection_name) == 0:
        logging.getLogger(__name__).error('Missing signaldb collection name')
        return None

    try:
        port = int(port)
    except ValueError:
        logging.getLogger(__name__).error('Port must be a positive integer')
    mongo_client = pymongo.MongoClient(host, port)
    db = mongo_client[collection_name]
    if len(user) > 0:
        db.authenticate(user, pwd, source='admin')
    logging.getLogger().debug('Connection with %s:%s established in: %fs' %
                              (host, port, time.perf_counter() - time_stamp))
    return db
