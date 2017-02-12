import json
import logging
import click
import signaldb

root_logger = logging.getLogger('')
root_logger.setLevel(logging.INFO)
console = logging.StreamHandler()
formatter = logging.Formatter('# %(asctime)s  %(levelname)s: %(name)s: %(message)s', datefmt='%Y%m%d %H:%M:%S')
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
@click.option('--db', default='market', help='Specify the database to connect to')
def upsert(input_files, props_merge_mode, series_merge_mode, host, port, user, pwd, db):
    conn = signaldb.get_db(host, port, user, pwd, db)
    signal_db = signaldb.SignalDb(conn)
    for input_file in input_files:
        try:
            with open(input_file, 'r') as f:
                instruments = json.load(f)
        except json.decoder.JSONDecodeError:
            logging.getLogger().error('Error parsing JSON in %s' % input_file)
            continue
        signal_db.upsert(instruments, props_merge_mode=props_merge_mode, series_merge_mode=series_merge_mode)


@cli.command('get')
@click.argument('source', nargs=1)
@click.argument('ticker', nargs=1)
@click.option('--host', default='', help='Specify mongodb host explicitly')
@click.option('--port', default='', help='Specify mongodb port explicitly')
@click.option('--user', default='', help='Specify mongodb user explicitly')
@click.option('--pwd', default='', help='Specify mongodb credentials explicitly')
@click.option('--db', default='market', help='Specify the database to connect to')
def get(source, ticker, host, port, user, pwd, db):
    conn = signaldb.get_db(host, port, user, pwd, db)
    signal_db = signaldb.SignalDb(conn)
    instrument = signal_db.get(source, ticker)
    if instrument is None:
        return
    click.echo(json.dumps(instrument, indent=4, sort_keys=True, cls=signaldb.JSONEncoderExtension))


@cli.command('list')
@click.argument('source', nargs=-1)
@click.option('--host', default='', help='Specify mongodb host explicitly')
@click.option('--port', default='', help='Specify mongodb port explicitly')
@click.option('--user', default='', help='Specify mongodb user explicitly')
@click.option('--pwd', default='', help='Specify mongodb credentials explicitly')
@click.option('--db', default='market', help='Specify the database to connect to')
def list_tickers(source, host, port, user, pwd, db):
    conn = signaldb.get_db(host, port, user, pwd, db)
    signal_db = signaldb.SignalDb(conn)
    ticker_list = []
    if len(source) == 0:
        ticker_list = signal_db.list_tickers()
    if len(source) == 1:
        ticker_list = signal_db.list_tickers(source[0])
    if len(source) > 1:
        return
    if ticker_list is None:
        return
    for ticker in ticker_list:
        click.echo('%s %s' % ticker)


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
    instruments = signal_db.find_instruments(filter_doc)
    click.echo(json.dumps(instruments, indent=4, sort_keys=True, cls=signaldb.JSONEncoderExtension))
