import datetime


def check_instrument(instrument):
    """Check if a finstrument object has a valid type."""
    if type(instrument) is not dict:
        return 1
    if not all([k in instrument.keys() for k in ['tickers', 'properties', 'series']]):
        return 2
    if type(instrument['tickers']) is not list:
        return 3
    if len(instrument['tickers']) == 0:
        return 4
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
        if '.' in series_key or '$' in series_key:
            return 15
    for prop_key in instrument['properties'].keys():
        if '.' in series_key or '$' in prop_key:
            return 16
    return 0


def consolidate(instruments, props_merge_mode='append'):
    """Return a list in which each instrument occurs exactly once"""
    ticker_map = {}
    properties_map = {}
    series_map = {}

    for instrument in instruments:
        ticker = tuple(instrument['tickers'][0])
        if ticker not in ticker_map.keys():
            ticker_map[ticker] = instrument['tickers']
        if ticker not in properties_map.keys():
            properties_map[ticker] = instrument['properties']
        else:
            merge_props(properties_map[ticker], instrument['properties'], props_merge_mode)
        if ticker not in series_map.keys():
            series_map[ticker] = {}
        for series_key in instrument['series'].keys():
            if series_key not in series_map[ticker].keys():
                series_map[ticker][series_key] = dict(instrument['series'][series_key])
            else:
                old_samples = series_map[ticker][series_key]
                new_samples = instrument['series'][series_key]
                for sample in new_samples:
                    key = sample[0]
                    old_samples[key] = sample[1]

    consolidated = []
    for ticker in ticker_map:
        instrument = dict(tickers=ticker_map[ticker], properties=properties_map[ticker], series={})
        for series_key in series_map[ticker].keys():
            series_dict = series_map[ticker][series_key]
            instrument['series'][series_key] = [[k, series_dict[k]] for k in sorted(list(series_dict.keys()))]
        consolidated.append(instrument)

    return consolidated


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


class Finstrument:
    def __init__(self, source: str, ticker: str):
        assert isinstance(source, str)
        assert isinstance(ticker, str)
        assert len(source) > 0 and len(ticker) > 0
        self._tickers = [(source, ticker), ]
        self._properties = {}
        self._series = {}

    def set_property(self, key: str, value):
        assert isinstance(key, str)
        assert len(key) > 0
        assert value is not None
        self._properties[key] = value

    def set_observation(self, series_name: str, observation_time: datetime.datetime, observation_value):
        assert observation_value is not None
        if series_name not in self._series.keys():
            self._series[series_name] = []
        self._series[series_name].append((observation_time, observation_value))

    def attach_series(self, series_name: str, series_data: list):
        assert len(series_name) > 0
        assert len(series_data) > 0
        self._series[series_name] = series_data

    def dump(self):
        return {'tickers': self._tickers, 'properties': self._properties, 'series': self._series}
