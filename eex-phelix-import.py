#!/bin/env python3

import json
import pprint
import rfc3339


def flatten(tree):
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


def str_to_datetime(s):
    return rfc3339.parse_datetime(s)


def set_field_types(contracts):
    cast_map = dict()
    cast_map['contract_field:delivery_from'] = str_to_datetime
    cast_map['contract_field:delivery_until'] = str_to_datetime
    cast_map['contract_field:trading_from'] = str_to_datetime
    cast_map['contract_field:trading_until'] = str_to_datetime
    cast_map['contract_field:expiry_date'] = str_to_datetime
    cast_map['contract_field:timestamp_of_occurrence'] = str_to_datetime

    processed_contracts = []
    for c in contracts:
        try:
            for key in c.keys() & cast_map.keys():
                c[key] = cast_map[key](c[key])
        except ValueError:
            continue
        processed_contracts.append(c)
    return processed_contracts


def categorize_fields(contracts):
    processed_contracts = []
    series_fields = ['noOfTradedContractsExchange', 'noOfTradedContractsOtc', 'noOfTradedContractsTotal',
                     'openInterestNoOfContracts', 'openInterestPrice', 'openInterestVolume', 'settlementPrice',
                     'volumeExchange', 'volumeOtc', 'volumeTotal']
    static_fields = ['contract_field:delivery_from', 'contract_field:delivery_until',
                     'contract_field:expiry_date', 'contract_field:product_code',
                     'contract_field:trading_from', 'contract_field:trading_until', 'product_field:currency',
                     'product_field:identifier', 'product_field:name', 'product_field:unit', 'contract_field:volume']
    ticker_fields = ['external_code:bloomberg', 'external_code:reuters', 'contract_field:identifier']
    name_mapping = {k: k for k in series_fields + static_fields + ticker_fields}
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
        contract = {'static': {}, 'series': {}}
        try:
            contract['t'] = c['contract_field:timestamp_of_occurrence']
            contract['series'] = {name_mapping[k]: c[k] for k in series_fields}
            contract['static'] = {name_mapping[k]: c[k] for k in static_fields}
            contract['static']['ticker'] = {name_mapping[k]: c[k] for k in ticker_fields}
        except LookupError:
            continue
        processed_contracts.append(contract)
    return processed_contracts


if __name__ == "__main__":
    input_file = r'/home/julian/sync/kumo/scrapyard/20161122-phelix-futures-detail.json'
    with open(input_file, 'r') as f:
        data = json.load(f)

    data_1 = flatten(data)
    data_2 = set_field_types(data_1)
    data_3 = categorize_fields(data_2)
    pprint.pprint(data_3)

"""
{
'contract_field:timestamp_of_occurrence': '2016-11-22T18:52:57+01:00',

 'noOfTradedContractsExchange': 0.0,
 'noOfTradedContractsOtc': 0.0,
 'noOfTradedContractsTotal': 0.0,
 'openInterestNoOfContracts': 300.0,
 'openInterestPrice': 13614.0,
 'openInterestVolume': 3600.0,
 'settlementPrice': 45.38,
 'volumeExchange': 0.0,
 'volumeOtc': 0.0,
 'volumeTotal': 0.0
'contract_field:volume': 12.0,

 'contract_field:contract_code': '2016.11',
 'contract_field:delivery_from': '2016-11-22T00:00:00+01:00',
 'contract_field:delivery_until': '2016-11-22T23:59:59+01:00',
 'contract_field:expiry_date': '2016-11-22T00:00:00+01:00',
 'contract_field:identifier': 'C-Power-F-DEAT-Peak-Day-2016.11.22',
 'contract_field:product_code': 'FP22',
 'contract_field:trading_from': '2016-11-14T00:00:00+01:00',
 'contract_field:trading_until': '2016-11-21T23:59:59+01:00',

 'product_field:currency': 'EUR',
 'product_field:identifier': 'P-Power-F-DEAT-Peak-Day',
 'product_field:name': 'Phelix Peak Day Future',
 'product_field:unit': 'EUR/MWh',

 'external_code:bloomberg': 'FPDAILY Comdty',
 'external_code:reuters': '0#EEXFP:',
 }
"""
