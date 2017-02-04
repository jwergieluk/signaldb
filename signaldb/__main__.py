#!/bin/env python3

import datetime
import json
import logging
from pprint import pprint

import click
import signaldb

root_logger = logging.getLogger('')
root_logger.setLevel(logging.INFO)
console = logging.StreamHandler()
formatter = logging.Formatter('# %(levelname)s | %(asctime)s | %(name)s | %(message)s', datefmt='%Y%m%d %H:%M:%S')
console.setFormatter(formatter)
root_logger.addHandler(console)


@click.group()
@click.version_option()
def cli():
    """signaldb   ..---...~~.. """


@cli.command('upsert')
@click.argument('input_files', nargs=-1)
@click.option('--merge_props_mode', default='append', help="Do not update existing instruments")
@click.option('--host', default='', help='Specify mongodb host explicitly')
@click.option('--port', default='', help='Specify mongodb port explicitly')
@click.option('--user', default='', help='Specify mongodb user explicitly')
@click.option('--pwd', default='', help='Specify mongodb credentials explicitly explicitly')
@click.option('--db', default='market', help='Specify the database to connect to')
def upsert(input_files, merge_props_mode, host, port, user, pwd, db):
    conn = signaldb.get_db(host, port, user, pwd, db)
    signal_db = signaldb.SignalDb(conn)
    for input_file in input_files:
        try:
            with open(input_file, 'r') as f:
                instruments = json.load(f)
        except json.decoder.JSONDecodeError:
            logging.getLogger().error('Error parsing JSON in %s' % input_file)
            continue
        signal_db.upsert(instruments, merge_props_mode)


@cli.command('get')
@click.argument('source', nargs=1)
@click.argument('ticker', nargs=1)
@click.option('--host', default='', help='Specify mongodb host explicitly')
@click.option('--port', default='', help='Specify mongodb port explicitly')
@click.option('--user', default='', help='Specify mongodb user explicitly')
@click.option('--pwd', default='', help='Specify mongodb credentials explicitly')
@click.option('--db', default='market', help='Specify the database to connect to')
def get_props(source, ticker, host, port, user, pwd, db):
    conn = signaldb.get_db(host, port, user, pwd, db)
    signal_db = signaldb.SignalDb(conn)
    pprint(signal_db.get(source, ticker))


@cli.command('find')
@click.argument('filter_doc', nargs=1)
@click.option('--host', default='', help='Specify mongodb host explicitly')
@click.option('--port', default='', help='Specify mongodb port explicitly')
@click.option('--user', default='', help='Specify mongodb user explicitly')
@click.option('--pwd', default='', help='Specify mongodb credentials explicitly explicitly')
@click.option('--db', default='market', help='Specify the database to connect to')
def find(filter_doc, host, port, user, pwd, db):
    try:
        filter_doc = json.loads(filter_doc)
    except json.decoder.JSONDecodeError:
        logging.getLogger().error('Error parsing search query')
        return
    conn = signaldb.get_db(host, port, user, pwd, db)
    signal_db = signaldb.SignalDb(conn)
    pprint(signal_db.find_instruments(filter_doc))


@cli.command('exportba')
@click.option('--host', default='', help='Specify mongodb host explicitly')
@click.option('--port', default='', type=int, help='Specify mongodb port explicitly')
@click.option('--user', default='', help='Specify mongodb user explicitly')
@click.option('--pwd', default='', help='Specify mongodb credentials explicitly explicitly')
@click.option('--db', default='market', help='Specify the database to connect to')
@click.option('--day', type=int)
def exportba(host, port, user, pwd, db, day):
    eex = signaldb.get_db(host, port, user, pwd, db)
    curves = eex['BidAskCurves'].find({'Day': day})
    instr_dict = {}
    for curve in curves:
        ticker_name = 'spot-curve-%s-%s' % (curve['MarketAreaName'], curve['TimeStepID'])
        ticker = ('epex-spot', ticker_name)

        props = {'MarketAreaName': curve['MarketAreaName'], 'TimeStepID': curve['TimeStepID'],
                 'category': 'epex-spot-curves'}
        timestamp = str(curve['Day'])
        timestamp = datetime.datetime.strptime(timestamp, "%Y%m%d")
        timestamp.replace(hour=12, minute=0, second=0, microsecond=0)

        if ticker not in instr_dict.keys():
            series = {'PurchasePrice': [], 'PurchaseVolume': [], 'SellPrice': [], 'SellVolume': []}
            instr_dict[ticker] = dict(properties=props, tickers=[ticker], series=series)

        series = instr_dict[ticker]['series']
        series['PurchasePrice'].append([timestamp, curve['PurchasePrice']])
        series['PurchaseVolume'].append([timestamp, curve['PurchaseVolume']])
        series['SellPrice'].append([timestamp, curve['SellPrice']])
        series['SellVolume'].append([timestamp, curve['SellVolume']])

    output_str = json.dumps(list(instr_dict.values()), cls=signaldb.JSONEncoderExtension)
    print(output_str)

