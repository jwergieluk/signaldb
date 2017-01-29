#!/bin/env python3

import pymongo
import pymongo.errors
import signaldb
import click
import logging
import os
import json
from pprint import pprint


root_logger = logging.getLogger('')
root_logger.setLevel(logging.INFO)
console = logging.StreamHandler()
formatter = logging.Formatter('# %(levelname)s | %(asctime)s | %(name)s | %(message)s', datefmt='%Y%m%d %H:%M:%S')
console.setFormatter(formatter)
root_logger.addHandler(console)


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
    if len(port) != 0:
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
    conn = get_db(host, port, user, pwd, db)
    signal_db = signaldb.SignalDb(conn)
    for input_file in input_files:
        try:
            with open(input_file, 'r') as f:
                instruments = json.load(f)
        except json.decoder.JSONDecodeError:
            logging.getLogger().error('Error parsing JSON in %s' % input_file)
            continue
        signal_db.upsert(instruments, merge_props_mode)


@cli.command('get_props')
@click.argument('source', nargs=1)
@click.argument('ticker', nargs=1)
@click.option('--host', default='', help='Specify mongodb host explicitly')
@click.option('--port', default='', help='Specify mongodb port explicitly')
@click.option('--user', default='', help='Specify mongodb user explicitly')
@click.option('--pwd', default='', help='Specify mongodb credentials explicitly')
@click.option('--db', default='market', help='Specify the database to connect to')
def get_props(source, ticker, host, port, user, pwd, db):
    conn = get_db(host, port, user, pwd, db)
    signal_db = signaldb.SignalDb(conn)
    pprint(signal_db.get_properties(source, ticker))


@cli.command('get_series')
@click.argument('source', nargs=1)
@click.argument('ticker', nargs=1)
@click.option('--print_json', is_flag=True)
@click.option('--host', default='', help='Specify mongodb host explicitly')
@click.option('--port', default='', help='Specify mongodb port explicitly')
@click.option('--user', default='', help='Specify mongodb user explicitly')
@click.option('--pwd', default='', help='Specify mongodb credentials explicitly explicitly')
@click.option('--db', default='market', help='Specify the database to connect to')
def get_series(source, ticker, print_json, host, port, user, pwd, db):
    conn = get_db(host, port, user, pwd, db)
    signal_db = signaldb.SignalDb(conn)
    if print_json:
        pprint(signal_db.get_series(source, ticker))
    else:
        print(signal_db.get_pandas(source, ticker).to_csv(None, sep=' '))


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
    conn = get_db(host, port, user, pwd, db)
    signal_db = signaldb.SignalDb(conn)
    pprint(signal_db.find_instruments(filter_doc))
