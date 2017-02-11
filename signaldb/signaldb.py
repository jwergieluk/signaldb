import datetime
import pytz
import logging
import pymongo
import pymongo.errors
import signaldb
from bson.objectid import ObjectId


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

        try:
            self.db[self.refs_col].create_index(
                [('source', pymongo.ASCENDING), ('ticker', pymongo.ASCENDING)], unique=True, name='source_ticker_index')
            self.db[self.refs_col].create_index('instr_id', unique=False, name='instr_id_index')
            self.db[self.paths_col].create_index(
                [('k', pymongo.ASCENDING), ('r', pymongo.ASCENDING)],
                unique=True, name='k_r_index')
            self.db[self.sheets_col].create_index(
                [('k', pymongo.ASCENDING), ('t', pymongo.ASCENDING), ('r', pymongo.ASCENDING)],
                unique=True, name='k_t_r_index')
        except pymongo.errors.OperationFailure:
            self.logger.error('Cannot access the db')
            raise ConnectionAbortedError('Cannot access the db')

    def purge_db(self):
        """Remove all data from the database."""
        self.logger.debug('Removing all data from the db.')
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
        now = datetime.datetime.utcnow().replace(tzinfo=None)
#        now = now.replace(microsecond=0)
        filter_doc = {'source': source, 'ticker': ticker, 'valid_until': {'$gte': now}}
        ticker_record = self.db[self.refs_col].find_one(filter_doc)
        if ticker_record is None:
            self.logger.info('Ticker (%s,%s) not found.' % (source, ticker))
            return None
        instrument = dict(tickers=[[source, ticker], ])
        properties_record = self.db[self.paths_col].find_one({'k': ticker_record['props']},
                                                             sort=[('r', pymongo.DESCENDING)])
        if properties_record is None:
            self.logger.warning("The ticker (%s,%s) points to a non-existent properties document." % (source, ticker))
            instrument['properties'] = {}
        else:
            instrument['properties'] = properties_record['v']
        series_refs = self.db[self.paths_col].find_one({'k': ticker_record['series']},
                                                       sort=[('r', pymongo.DESCENDING)])
        if 'series_refs' is None:
            self.logger.warning('The instrument (%s,%s) has no series attached.' % (source, ticker))
            instrument['series'] = {}
            return instrument
        if len(series_refs['v']) == 0:
            self.logger.warning('Instrument (%s,%s) has no series attached.' % (source, ticker))
            return instrument

        series = {}
        for ref in series_refs['v'].items():
            observations = self.__get_series(ref[1])
            if len(observations) > 0:
                series[ref[0]] = observations
            else:
                self.logger.warning('Series %s for the instrument (%s,%s) is empty.' % (ref[0], source, ticker))
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
            self.logger.error('%s str max length exceeded' % label_name)
            return False
        if len(label) == 0:
            self.logger.error('Given %s is empty' % label_name)
        return True

    def upsert(self, instruments, props_merge_mode='append', series_merge_mode='append'):
        """Update or insert a list of instruments."""
        if props_merge_mode not in ['append', 'replace']:
            self.logger.error('Requested properties merge mode is not supported yet.')
            return False
        if series_merge_mode not in ['append', 'replace']:
            self.logger.error('Requested series merge mode is not supported yet.')
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
            self.__upsert_instrument(instrument, props_merge_mode, series_merge_mode)
        return True

    def __upsert_instrument(self, instrument, props_merge_mode, series_merge_mode):
        """Update or insert an instrument"""
        now = datetime.datetime.utcnow().replace(tzinfo=None)  # TODO replace with with server time
#        now = now.replace(microsecond=0)
        main_ref = self.__find_one_ref(instrument['tickers'], now)

        if main_ref is None:
            self.__insert_instrument(instrument, now)
        else:
            self.__update_instrument(instrument, main_ref, props_merge_mode, series_merge_mode, now)

        # Remove helper fields added to the input instrument object
#        instrument['properties'].pop('series', None)
#        instrument['properties'].pop('_id', None)
        return True

    def __find_one_ref(self, tickers, now):
        for ticker in tickers:
            filter_doc = {'source': ticker[0], 'ticker': ticker[1], 'valid_until': {'$gte': now}}
            ticker_record = self.db[self.refs_col].find_one(filter_doc)
            if ticker_record is not None:
                return ticker_record
        return None

    def __insert_instrument(self, instrument, now):
        first_ticker = instrument['tickers'][0]
        self.logger.debug("Add new instrument with ticker (%s,%s)" % (first_ticker[0], first_ticker[1]))

        refs_to_insert = self.__prepare_refs(instrument['tickers'])
        props_id = refs_to_insert[0]['props']
        series_id = refs_to_insert[0]['series']
        scenarios_id = refs_to_insert[0]['scenarios']

        props_obj = {'_id': ObjectId(), 'k': props_id, 'r': now, 'v': instrument['properties']}
        series_refs = {key: ObjectId() for key in instrument['series'].keys()}
        series_obj = {'_id': ObjectId(), 'k': series_id, 'r': now, 'v': series_refs}

        flat_series = []
        for key in series_refs:
            series_data = instrument['series'][key]
            for sample in series_data:
                flat_series.append({'k': series_refs[key], 'r': now,
                                    't': sample[0].replace(microsecond=0), 'v': sample[1]})
        try:
            self.db[self.refs_col].insert(refs_to_insert)
            self.db[self.paths_col].insert_one(props_obj)
            self.db[self.paths_col].insert_one(series_obj)
        except KeyboardInterrupt:
            # TODO add revision-aware unwind (low priority)
            self.db[self.refs_col].delete_many({'_id': {'$in': [t['_id'] for t in refs_to_insert]}})
            raise
        self.__upsert_series(flat_series)

    def __update_instrument(self, instrument, main_ref, props_merge_mode, series_merge_mode, now):
        props = self.db[self.paths_col].find_one({'k': main_ref['props']}, sort=[('r', pymongo.DESCENDING)])
        if props is None:
            props = dict(k=main_ref['props'], r=now, v=instrument['properties'])
            update_props = True
        else:
            update_props = merge_props(props['v'], instrument['properties'], props_merge_mode)
        type(self).__clean_fields_path_obj(props)

        series_refs = self.db[self.paths_col].find_one({'k': main_ref['series']}, sort=[('r', pymongo.DESCENDING)])
        update_series_refs = False
        if series_refs is None:
            update_series_refs = True
            series_refs = {'_id': ObjectId(), 'k': main_ref['series'], 'r': now, 'v': {}}
        else:
            type(self).__clean_fields_path_obj(series_refs)

        flat_series = []
        for key in instrument['series'].keys():
            series_data = instrument['series'][key]
            series_id_ = ObjectId()
            if key not in series_refs['v'].keys():
                update_series_refs = True
                series_refs['v'][key] = series_id_
                for sample in series_data:
                    flat_series.append({'k': series_id_, 'r': now,
                                        't': sample[0].replace(microsecond=0), 'v': sample[1]})
            else:
                current_series_data = self.__get_series(series_refs['v'][key])
                merged_series = self.__merge_series(current_series_data, instrument['series'][key])
                for sample in merged_series:
                    flat_series.append({'k': series_id_, 'r': now,
                                        't': sample[0].replace(microsecond=0), 'v': sample[1]})
        if series_merge_mode == 'replace':
            for key in set(series_refs['v'].keys()) - set(instrument['series'].keys()):
                series_refs['v'].pop(key, None)
                update_series_refs = True
        if update_props:
            props['r'] = now
            self.db[self.paths_col].replace_one(dict(k=props['k'], r=now), props, upsert=True)
        if update_series_refs:
            series_refs['r'] = now
            self.db[self.paths_col].replace_one(dict(k=series_refs['k'], r=now), series_refs, upsert=True)
        self.__upsert_series(flat_series)

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

    @staticmethod
    def __prepare_refs(tickers):
        props_id = ObjectId()
        series_id = ObjectId()
        scenarios_id = ObjectId()

        refs = []
        for ticker in tickers:
            refs.append(dict(_id=ObjectId(),
                             source=ticker[0],
                             ticker=ticker[1],
                             valid_until=datetime.datetime.max,
                             props=props_id,
                             series=series_id,
                             scenarios=scenarios_id))
        return refs

    def __get_series(self, series_key):
        series = []
        cursor = self.db[self.sheets_col].find({'k': series_key})
#        pipeline = []
#        pipeline.append({'$group': {'_id': {'t'}}})
#        cursor = self.db[self.sheets_col].aggregate(pipeline=pipeline)
        for item in cursor:
            series.append([item['t'], item['v']])
        return series

    def __merge_series(self, old_series, new_series):
        old_series_dict = dict(old_series)
        new_series_dict = dict(new_series)
        series = []
        for t in new_series_dict.keys():
            if t in old_series_dict.keys():
                if old_series_dict[t] == new_series_dict[t]:
                    continue
            series.append([t, new_series_dict[t]])
        return series

    @staticmethod
    def __clean_fields_path_obj(obj: dict):
        """Remove _id field and all fields not belonging to a path obj"""
        for key in list(obj.keys()):
            if key not in ['k', 'r', 'v']:
                obj.pop(key, None)


def merge_props(old_props: dict, new_props: dict, merge_mode: str):
    """Add new properties to a given properties document."""
    assert merge_mode in ['append', 'replace']
    current_props_modified = False
    if merge_mode == 'append':
        for key in new_props:
            if key not in old_props.keys():
                old_props[key] = new_props[key]
                current_props_modified = True
    if merge_mode == 'replace':
        for key in new_props.keys():
            old_props[key] = new_props[key]
            current_props_modified = True
        for key in set(old_props.keys()) - set(new_props.keys()):
            if key in ['series', '_id']:
                continue
            old_props.pop(key, None)
            current_props_modified = True
    return current_props_modified


def get_utc_datetime(d: datetime.datetime):
    utc_time_zone = pytz.timezone('UTC')
    return d.astimezone(utc_time_zone).replace(tzinfo=None)

