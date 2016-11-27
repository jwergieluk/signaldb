#!/bin/env python3

import json
import os
import logging
import traceback
import pytz
import rfc3339
import signaldb
import datetime
from bson.objectid import ObjectId
from collections import OrderedDict


def str_to_datetime(s):
    d = rfc3339.parse_datetime(s)
    utc_time_zone = pytz.timezone('UTC')
    return d.astimezone(utc_time_zone).replace(tzinfo=None)


def str_to_ticker_list(s):
    if '/' not in s:
        return s
    return s.split('/')


def get_market_data_dir(*dirs):
    target_dir = os.path.join(os.environ['market_data_dir'], *dirs)
    os.makedirs(target_dir, exist_ok=True)
    return target_dir


class JSONEncoderExtension(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return rfc3339.datetimetostr(obj)
        if isinstance(obj, ObjectId):
            return str(obj)

        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


class ProcessFuturesDetail:
    market_db = "eex"
    target_collection = "eex.phelix.futures"

    @classmethod
    def flatten(cls, tree):
        try:
            tree = tree['data']
        except LookupError:
            return []
        level_1 = []
        for p in tree:
            try:
                level_1 += p['rows']
            except LookupError:
                pass
        level_2 = []
        for p in level_1:
            try:
                level_2.append(p['data'])
            except ValueError:
                pass
        return level_2

    @classmethod
    def set_field_types(cls, contracts):
        cast_map = dict()
        cast_map['contract_field:delivery_from'] = str_to_datetime
        cast_map['contract_field:delivery_until'] = str_to_datetime
        cast_map['contract_field:trading_from'] = str_to_datetime
        cast_map['contract_field:trading_until'] = str_to_datetime
        cast_map['contract_field:expiry_date'] = str_to_datetime
        cast_map['contract_field:timestamp_of_occurrence'] = str_to_datetime
        cast_map['external_code:bloomberg'] = str_to_ticker_list
        cast_map['external_code:reuters'] = str_to_ticker_list

        processed_contracts = []
        for c in contracts:
            try:
                for key in c.keys() & cast_map.keys():
                    c[key] = cast_map[key](c[key])
            except ValueError:
                print("# ERROR: set_field_types: Invalid contract %s." % c.__str__())
                continue
            processed_contracts.append(c)
        return processed_contracts

    @classmethod
    def categorize_fields(cls, contracts):
        processed_contracts = []
        series_fields = {'noOfTradedContractsExchange', 'noOfTradedContractsOtc', 'noOfTradedContractsTotal',
                         'openInterestNoOfContracts', 'openInterestPrice', 'openInterestVolume', 'settlementPrice',
                         'volumeExchange', 'volumeOtc', 'volumeTotal'}
        static_fields = {'contract_field:delivery_from', 'contract_field:delivery_until',
                         'contract_field:expiry_date', 'contract_field:product_code',
                         'contract_field:trading_from', 'contract_field:trading_until', 'product_field:currency',
                         'product_field:identifier', 'product_field:name', 'product_field:unit', 'contract_field:volume'}
        ticker_fields = {'external_code:bloomberg', 'external_code:reuters', 'contract_field:identifier'}
        required_fields = {'settlementPrice', 'volumeTotal', 'contract_field:identifier', 'contract_field:delivery_from',
                           'contract_field:delivery_until', 'contract_field:volume'}
        name_mapping = {k: k for k in series_fields | static_fields | ticker_fields}
        name_mapping['contract_field:contract_code'] = 'contract_code'
        name_mapping['contract_field:delivery_from'] = 'delivery_from'
        name_mapping['contract_field:delivery_until'] = 'delivery_until'
        name_mapping['contract_field:expiry_date'] = 'expiry_date'
        name_mapping['contract_field:identifier'] = 'eex'
        name_mapping['contract_field:product_code'] = 'product_code'
        name_mapping['contract_field:trading_from'] = 'trading_from'
        name_mapping['contract_field:trading_until'] = 'trading_until'
        name_mapping['product_field:currency'] = 'currency'
        name_mapping['product_field:identifier'] = 'product_identifier'
        name_mapping['product_field:name'] = 'product_name'
        name_mapping['product_field:unit'] = 'unit'
        name_mapping['external_code:bloomberg'] = 'bloomberg'
        name_mapping['external_code:reuters'] = 'reuters'
        name_mapping['contract_field:volume'] = 'contract_volume'
        for c in contracts:
            if not all([k in c.keys() for k in required_fields]):
                continue
            if c['volumeTotal'] <= 0.0:
                continue
            contract = {'static': {}, 'series': {}}
            try:
                contract['t'] = c['contract_field:timestamp_of_occurrence']
                series_dict = {name_mapping[k]: c[k] for k in series_fields & c.keys()}
                static_dict = {name_mapping[k]: c[k] for k in static_fields & c.keys()}
                ticker_dict = {name_mapping[k]: c[k] for k in ticker_fields & c.keys()}
                contract['series'] = OrderedDict(sorted(series_dict.items(), key=lambda k: k[0]))
                contract['static'] = OrderedDict(sorted(static_dict.items(), key=lambda k: k[0]))
                contract['static']['ticker'] = OrderedDict(sorted(ticker_dict.items(), key=lambda k: k[0]))
            except LookupError:
                print("# ERROR: categorize_fields: Invalid contract %s." % c.__str__())
                print(traceback.format_exc())
                continue
            processed_contracts.append(contract)
        return processed_contracts

    @classmethod
    def correct_observation_time(cls, contracts, observation_date):
        for c in contracts:
            if c['t'].date() != observation_date.date():
                observation_date = observation_date.replace(hour=17, minute=30)
                c['t'] = observation_date

    @classmethod
    def run(cls):
        signal_db = signaldb.SignalDb()
        input_dir = get_market_data_dir('eex-phelix-futures-detail', '1')
        output_dir = get_market_data_dir('eex-phelix-futures-detail', '2')
        for input_file in os.listdir(input_dir):
            print('# INFO: Processing %s.' % input_file)
            with open(os.path.join(input_dir, input_file), 'r') as f:
                data = json.load(f)

            data_1 = cls.flatten(data)
            data_2 = cls.set_field_types(data_1)
            data_3 = cls.categorize_fields(data_2)
            date_from_file_name = datetime.datetime.strptime(input_file.split("-")[0], "%Y%m%d")
            cls.correct_observation_time(data_3, date_from_file_name)
            for instrument in data_3:
                signal_db.try_save_instrument(instrument, cls.market_db, cls.target_collection)

            output_file = os.path.join(output_dir, input_file)
            if os.path.exists(output_file):
                print("# ERROR: Output file \"%s\" not overwritten." % output_file)
                continue
            with open(output_file, 'w') as g:
                json.dump(data_3, g, cls=JSONEncoderExtension)


class ProcessFuturesPrices:
    phelix_futures_collection = "eex.phelix.futures"

    @classmethod
    def run(cls):
        signal_db = signaldb.SignalDb()
        input_dir = get_market_data_dir('eex-phelix-futures-prices', '1')
        for input_file in os.listdir(input_dir):
            logger.info('Processing %s.' % input_file)
            with open(os.path.join(input_dir, input_file), 'r') as f:
                data = json.load(f)

            try:
                time_series = ProcessFuturesPrices.extract_time_series(data)
                ticker = ProcessFuturesPrices.extract_ticker(data)
            except LookupError:
                logger.error("ProcessFuturesPrices: %s has unexpected structure." % input_file)
                continue
            except ValueError:
                logger.error("ProcessFuturesPrices: Error while parsing data in %s." % input_file)
                continue

            signal_db.append_series_to_instrument('eex', ticker, 'price', time_series, cls.phelix_futures_collection)
            break

    @classmethod
    def extract_time_series(cls, data):
        time_value_pairs = []
        for observation in data['series']:
            time_stamp = datetime.datetime.fromtimestamp(observation[0]/1000)
            value = observation[1]
            time_value_pairs.append([time_stamp, value])
        return time_value_pairs

    @classmethod
    def extract_ticker(cls, data):
        return data['contractIdentifier']


if __name__ == "__main__":
    logger = logging.getLogger('eex_phelix_import')
    root_logger = logging.getLogger('')
    root_logger.setLevel(logging.DEBUG)
    console = logging.StreamHandler()
    formatter = logging.Formatter('# %(levelname)s | %(asctime)s | %(name)s | %(message)s', datefmt='%Y%m%d %H:%M:%S')
    console.setFormatter(formatter)
    root_logger.addHandler(console)

    ProcessFuturesPrices.run()

