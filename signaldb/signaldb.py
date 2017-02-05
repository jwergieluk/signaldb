import datetime
import logging
import pymongo
import pymongo.errors
import signaldb
from bson.objectid import ObjectId


def merge_props(current_props: dict, new_props: dict, merge_props_mode: str):
    """Add new properties to a given properties document."""
    assert merge_props_mode in ['append', 'replace']
    current_props_modified = False
    if merge_props_mode == 'append':
        for key in new_props:
            if key not in current_props.keys():
                current_props[key] = new_props[key]
                current_props_modified = True
    if merge_props_mode == 'replace':
        for key in new_props.keys():
            current_props[key] = new_props[key]
            current_props_modified = True
        for key in set(current_props.keys()) - set(new_props.keys()):
            if key in ['series', '_id']:
                continue
            current_props.pop(key, None)
            current_props_modified = True
    return current_props_modified


class SignalDb:
    def __init__(self, db):
        self.logger = logging.getLogger(__name__)
        self.db = db
        self.refs_col = 'refs'
        self.paths_col = 'paths'
        self.sheets_col = 'sheets'
        self.spaces_col = 'spaces'
        self.source_max_len = 256
        self.ticker_max_len = 256

        self.db[self.refs_col].create_index(
            [('source', pymongo.ASCENDING), ('ticker', pymongo.ASCENDING)], unique=True, name='source_ticker_index')
        self.db[self.refs_col].create_index('instr_id', unique=False, name='instr_id_index')
        self.db[self.sheets_col].create_index(
            [('k', pymongo.ASCENDING), ('t', pymongo.ASCENDING)], unique=True, name='k_t_index')

    def purge_db(self):
        """Remove all data from the database."""
        self.logger.debug('Removing all data from db.')
        self.db[self.refs_col].delete_many({})
        self.db[self.sheets_col].delete_many({})
        self.db[self.paths_col].delete_many({})

    @staticmethod
    def check_instrument(instrument):
        """Check if an instrument object has a valid type."""
        if type(instrument) is not dict:
            return 1
        if not all([k in instrument.keys() for k in ['tickers', 'properties', 'series']]):
            return 2
        if type(instrument['tickers']) is not list:
            return 3
        if len(instrument['tickers']) == 0:
            return 3
        for ticker in instrument['tickers']:
            if type(ticker) not in [list, tuple]:
                return 5
            if len(ticker) != 2:
                return 6
            if not all([type(ticker_part) is str for ticker_part in ticker]):
                return 7
            if not all([len(ticker_part) > 0 for ticker_part in ticker]):
                return 8
        if type(instrument['series']) is not dict:
            return 9
        for series_key in instrument['series']:
            if type(series_key) is not str:
                return 10
            if len(series_key) == 0:
                return 11
            series = instrument['series'][series_key]
            for sample in series:
                if type(sample) not in [list, tuple]:
                    return 12
                if len(sample) != 2:
                    return 13
                if type(sample[0]) not in [datetime.date, datetime.datetime]:
                    return 14
        return 0

    def rename(self, source_old: str, ticker_old: str, source_new: str, ticker_new: str):
        """Alter a ticker"""
        pass

    def delete(self, source: str, ticker: str):
        """Delete an instrument"""
        pass

    def list_tickers(self, source=''):
        """Return a list of all available tickers matching a given source"""
        if type(source) is not str:
            self.logger.error('Source must be a string')
            return None
        if len(source) > self.source_max_len:
            self.logger.error('source str length exceeded')
            return None
        if len(source) == 0:
            cursor = self.db[self.refs_col].find({})
        else:
            cursor = self.db[self.refs_col].find({'source': source})
        if cursor is None:
            self.logger.error('Error querying the db')
            return None
        ticker_list = []
        for label in cursor:
            if 'source' not in label.keys() or 'ticker' not in label.keys():
                self.logger.error('Erroneous ticker document found. Check the db!')
                return None
            ticker_list.append((label['source'], label['ticker']))
        return ticker_list

    def find_instruments(self, filter_doc):
        cursor = self.db[self.paths_col].find(filter_doc, limit=10000)
        instruments = []
        for instrument in cursor:
            ticker_cursor = self.db[self.refs_col].find({'instr_id': instrument['_id']})
            tickers = [(ticker['source'], ticker['ticker']) for ticker in ticker_cursor]
            instrument['tickers'] = tickers
            instruments.append(instrument)
        return instruments

    def get(self, source: str, ticker: str):
        """Find a single instrument and return it in the standard form"""
        ticker_record = self.db[self.refs_col].find_one({'source': source, 'ticker': ticker})
        if ticker_record is None:
            self.logger.info("Ticker (%s,%s) not found. " % (source, ticker))
            return None
        properties = self.db[self.paths_col].find_one({'_id': ticker_record['instr_id']})
        if properties is None:
            self.logger.warning("The ticker (%s,%s) points to a non-existent properties document." % (source, ticker))
            return None
        instrument = dict(properties=properties, tickers=[[source, ticker], ])
        if 'series' not in properties.keys():
            self.logger.warning('The instrument (%s,%s) has no series attached.' % (source, ticker))
            instrument['series'] = {}
            return instrument
        instrument['series'] = properties.pop('series')
        if len(instrument['series']) == 0:
            self.logger.warning('Instrument (%s,%s) has no series attached.' % (source, ticker))
            return instrument
        series = {}
        for ref in instrument['series'].items():
            observations = []
            cursor = self.db[self.sheets_col].find({'k': ref[1]})
            for item in cursor:
                observations.append([item['t'], item['v']])
            series[ref[0]] = observations
        instrument['series'] = series
        return instrument

    def __validate_source_ticker(self, source: str, ticker: str):
        return self.__validate_label(source, self.source_max_len, 'source') and \
               self.__validate_label(ticker, self.ticker_max_len, 'ticker')

    def __validate_label(self, label: str, max_len: int, label_name: str):
        if type(label) is not str:
            self.logger.error('%s must be a str' % label_name)
            return False
        if len(label) > max_len:
            self.logger.error('%s str max length exceeded')
            return False
        if len(label) == 0:
            self.logger.error('Given %s is empty')
        return True

    def upsert(self, instruments, merge_props_mode='append'):
        """Update or insert a list of instruments."""
        if merge_props_mode not in ['append', 'replace']:
            self.logger.error('Requested merge mode is not supported yet.')
            return False
        if type(instruments) not in [list, tuple, dict]:
            self.logger.error("upsert: supplied instrument data is not dict, list, or tuple")
            return False
        if type(instruments) is dict:
            instruments = [instruments]
        signaldb.recursive_str_to_datetime(instruments)
        for i, instrument in enumerate(instruments):
            check_result = self.check_instrument(instrument)
            if check_result != 0:
                self.logger.error("Supplied instrument has wrong type (index no %d; failed test %d)." %
                                  (i+1, check_result))
                continue
            self.__upsert_instrument(instrument, merge_props_mode)
        return True

    def __upsert_instrument(self, instrument, merge_props_mode):
        """Update or insert an instrument"""
        main_ref = self.__find_one_ref(instrument['tickers'])

        if main_ref is None:
            self.__insert_instrument(instrument)
        else:
            self.__update_instrument(instrument, main_ref, merge_props_mode)

        # Remove helper fields added to the input instrument object
        instrument['properties'].pop('series', None)
        instrument['properties'].pop('_id', None)
        return True

    def __upsert_series(self, series):
        """Insert a list of observations to the series col. Updates existing observations."""
        if len(series) == 0:
            return
        try:
            self.db[self.sheets_col].insert_many(series)
        except pymongo.errors.BulkWriteError:
            for sample in series:
                sample.pop('_id', None)
                self.db[self.sheets_col].find_one_and_replace(
                    {'k': sample['k'], 't': sample['t']}, sample, upsert=True)

    def list_dangling_series(self):
        pass

    def remove_dangling_series(self):
        pass

    def __find_one_ref(self, tickers):
        for ticker in tickers:
            ticker_record = self.db[self.refs_col].find_one({'source': ticker[0], 'ticker': ticker[1]})
            if ticker_record is not None:
                return ticker_record
        return None

    def __insert_instrument(self, instrument):
        first_ticker = instrument['tickers'][0]
        self.logger.debug("Add new instrument with ticker (%s,%s)" % (first_ticker[0], first_ticker[1]))
        instrument_id = ObjectId()

        tickers_for_insert = [{'_id': ObjectId(), 'source': ticker[0], 'ticker': ticker[1],
                               'instr_id': instrument_id} for ticker in instrument['tickers']]
        instrument['properties']['_id'] = instrument_id
        instrument['properties']['series'] = {series_key: ObjectId() for series_key in instrument['series'].keys()}

        flat_series = []
        for key in instrument['series'].keys():
            series = instrument['series'][key]
            series_id = instrument['properties']['series'][key]
            for sample in series:
                flat_series.append({'k': series_id, 't': sample[0], 'v': sample[1]})
        try:
            self.db[self.refs_col].insert(tickers_for_insert)
            self.db[self.paths_col].insert_one(instrument['properties'])
        except KeyboardInterrupt:
            self.db[self.refs_col].delete_many({'_id': {'$in': [t['_id'] for t in tickers_for_insert]}})
            raise
        self.__upsert_series(flat_series)

    def __update_instrument(self, instrument, main_ref, merge_props_mode):
        instrument_id = main_ref['instr_id']
        current_props = self.db[self.paths_col].find_one({'_id': instrument_id})
        if current_props is None:
            self.logger.warning('Repair the dangling ticker (%s,%s)' %
                                (main_ref['source'], main_ref['ticker']))
            self.db[self.refs_col].delete_one(main_ref)
            return self.__upsert_instrument(instrument, merge_props_mode)
        updated = merge_props(current_props, instrument['properties'], merge_props_mode)

        flat_series = []
        for key in instrument['series'].keys():
            series = instrument['series'][key]
            series_id = ObjectId()
            if key not in current_props['series'].keys():
                updated = True
                current_props['series'][key] = series_id
            else:
                series_id = current_props['series'][key]
            for sample in series:
                flat_series.append({'k': series_id, 't': sample[0], 'v': sample[1]})
        for key in set(current_props['series'].keys()) - set(instrument['series'].keys()):
            removed_series_id = current_props['series'].pop(key, None)
            self.db[self.sheets_col].delete_many({'_id': removed_series_id})
            updated = True
        if updated:
            self.db[self.paths_col].replace_one({'_id': current_props['_id']}, current_props)
        self.__upsert_series(flat_series)

