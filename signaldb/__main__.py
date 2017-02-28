import json
import logging
import click
import rfc3339

import signaldb
import time


root_logger = logging.getLogger('')
root_logger.setLevel(logging.INFO)
console = logging.StreamHandler()
formatter = logging.Formatter('# %(asctime)s %(levelname)s: %(name)s: %(message)s', datefmt='%Y%m%d %H:%M:%S')
console.setFormatter(formatter)
root_logger.addHandler(console)


@click.group()
@click.version_option()
def cli():
    """signaldb   ..---...~~.. """


@cli.command('upsert')
@click.argument('input_files', nargs=-1)
@click.option('--props_merge_mode', default='append', help="Supported modes are 'append' (default) and 'replace'")
@click.option('--series_merge_mode', default='append', help="Supported modes are 'append' (default) and 'replace'")
@click.option('--host', default='', help='Specify mongodb host explicitly')
@click.option('--port', default='', help='Specify mongodb port explicitly')
@click.option('--user', default='', help='Specify mongodb user explicitly')
@click.option('--pwd', default='', help='Specify mongodb credentials explicitly explicitly')
@click.option('--db', default='', help='Specify the database to connect to')
@click.option('--consolidate-input/--no-consolidate-input', default=True, help='Consolidate instruments.')
@click.option('--debug/--no-debug', default=False, help='Show debug messages')
def upsert(input_files, props_merge_mode, series_merge_mode, host, port, user, pwd, db, consolidate_input, debug):
    click.echo('Checkpoint: %s' % rfc3339.datetimetostr(signaldb.get_utc_now()))
    if debug:
        root_logger.setLevel(logging.DEBUG)
    conn = signaldb.get_db(host, port, user, pwd, db)
    sdb = signaldb.SignalDb(conn)
    time_stamp = time.perf_counter()
    try:
        instruments = read_instruments(input_files)
    except FileNotFoundError:
        logging.getLogger(__name__).error('File not found.')
        return
    sdb.upsert(instruments, props_merge_mode=props_merge_mode, series_merge_mode=series_merge_mode,
               consolidate_flag=consolidate_input)
    logging.getLogger(__name__).debug('Total execution time : %f' % (time.perf_counter() - time_stamp))


@cli.command('consolidate')
@click.argument('input_files', nargs=-1)
@click.argument('output_file', nargs=1, type=click.File('w'))
@click.option('--debug/--no-debug', default=False, help='Show debug messages')
def consolidate(input_files, output_file, debug):
    if debug:
        root_logger.setLevel(logging.DEBUG)
    time_stamp = time.perf_counter()
    instruments = read_instruments(input_files)
    json.dump(instruments, output_file, cls=signaldb.JSONEncoderExtension)
    logging.getLogger(__name__).debug('Total execution time : %f' % (time.perf_counter() - time_stamp))


def read_instruments(input_files):
    instruments = []
    for input_file in input_files:
        try:
            with open(input_file, 'r') as f:
                decoded_json = json.load(f)
            if type(decoded_json) is list:
                instruments += decoded_json
            else:
                instruments.append(decoded_json)
        except json.decoder.JSONDecodeError:
            logging.getLogger(__name__).error('Error parsing JSON in %s' % input_file)
            continue
    return instruments


@cli.command('rollback')
@click.argument('time-stamp', nargs=1)
@click.option('--host', default='', help='Specify mongodb host explicitly')
@click.option('--port', default='', help='Specify mongodb port explicitly')
@click.option('--user', default='', help='Specify mongodb user explicitly')
@click.option('--pwd', default='', help='Specify mongodb credentials explicitly')
@click.option('--db', default='', help='Specify the database to connect to')
def rollback(time_stamp, host, port, user, pwd, db, debug):
    if debug:
        root_logger.setLevel(logging.DEBUG)
    conn = signaldb.get_db(host, port, user, pwd, db)
    sdb = signaldb.SignalDb(conn)
    sdb.rollback(time_stamp)


@cli.command('get')
@click.argument('source', nargs=1)
@click.argument('ticker', nargs=1)
@click.option('--host', default='', help='Specify mongodb host explicitly')
@click.option('--port', default='', help='Specify mongodb port explicitly')
@click.option('--user', default='', help='Specify mongodb user explicitly')
@click.option('--pwd', default='', help='Specify mongodb credentials explicitly')
@click.option('--db', default='', help='Specify the database to connect to')
@click.option('--debug/--no-debug', default=False, help='Show debug messages')
def get(source, ticker, host, port, user, pwd, db, debug):
    if debug:
        root_logger.setLevel(logging.DEBUG)
    conn = signaldb.get_db(host, port, user, pwd, db)
    sdb = signaldb.SignalDb(conn)
    instrument = sdb.get(source, ticker)
    if instrument is None:
        return
    click.echo(json.dumps(instrument, indent=4, sort_keys=True, cls=signaldb.JSONEncoderExtension))


@cli.command('list')
@click.argument('source', nargs=-1)
@click.option('--host', default='', help='Specify mongodb host explicitly')
@click.option('--port', default='', help='Specify mongodb port explicitly')
@click.option('--user', default='', help='Specify mongodb user explicitly')
@click.option('--pwd', default='', help='Specify mongodb credentials explicitly')
@click.option('--db', default='', help='Specify the database to connect to')
def list_tickers(source, host, port, user, pwd, db):
    conn = signaldb.get_db(host, port, user, pwd, db)
    sdb = signaldb.SignalDb(conn)
    ticker_list = []
    if len(source) == 0:
        ticker_list = sdb.list_tickers()
    if len(source) == 1:
        ticker_list = sdb.list_tickers(source[0])
    if len(source) > 1:
        return
    if ticker_list is None:
        return
    for ticker in ticker_list:
        click.echo('%s %s' % ticker)


@cli.command('info')
@click.option('--host', default='', help='Specify mongodb host explicitly')
@click.option('--port', default='', help='Specify mongodb port explicitly')
@click.option('--user', default='', help='Specify mongodb user explicitly')
@click.option('--pwd', default='', help='Specify mongodb credentials explicitly')
@click.option('--db', default='', help='Specify the database to connect to')
def info(host, port, user, pwd, db):
    conn = signaldb.get_db(host, port, user, pwd, db)
    sdb = signaldb.SignalDb(conn)
    doc_count = sdb.count_items()
    click.echo('Object count: %d refs, %d paths, %d sheets.' % doc_count)
    click.echo('Checkpoint: %s' % rfc3339.datetimetostr(signaldb.get_utc_now()))


@cli.command('find')
@click.argument('filter_doc', nargs=1)
@click.option('--host', default='', help='Specify mongodb host explicitly')
@click.option('--port', default='', help='Specify mongodb port explicitly')
@click.option('--user', default='', help='Specify mongodb user explicitly')
@click.option('--pwd', default='', help='Specify mongodb credentials explicitly explicitly')
@click.option('--db', default='', help='Specify the database to connect to')
def find(filter_doc, host, port, user, pwd, db):
    try:
        filter_doc = json.loads(filter_doc)
    except json.decoder.JSONDecodeError:
        logging.getLogger().error('Error parsing search query')
        return
    conn = signaldb.get_db(host, port, user, pwd, db)
    sdb = signaldb.SignalDb(conn)
    instruments = sdb.find_instruments(filter_doc)
    click.echo(json.dumps(instruments, indent=4, sort_keys=True, cls=signaldb.JSONEncoderExtension))
