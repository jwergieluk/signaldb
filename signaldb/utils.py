import pytz
import rfc3339
import re
import datetime
import json


def str_to_datetime(s):
    d = rfc3339.parse_datetime(s)
    utc_time_zone = pytz.timezone('UTC')
    return d.astimezone(utc_time_zone).replace(tzinfo=None)


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


class JSONEncoderExtension(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return rfc3339.datetimetostr(obj)
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)