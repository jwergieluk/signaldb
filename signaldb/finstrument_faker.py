import datetime
import random


class Fake:
    letters = 'Racevgav LopNiskEl MabEcNiOg9 liubUpbi'

    @classmethod
    def company(cls):
        return "".join(random.choices(cls.letters, k=20)).strip().capitalize()

    @classmethod
    def country_code(cls):
        return "".join(random.choices(cls.letters, k=2)).upper()

    @classmethod
    def id(cls, k):
        return "".join(random.choices('ABCDEF1234567890', k=k)).upper()


class FinstrumentFaker:
    """Random financial instrument generator"""
    time_series_len = 365

    @classmethod
    def get(cls, n=1):
        instruments = []
        for i in range(n):
            instrument = {'tickers': cls.get_tickers(), 'properties': cls.get_props(),
                          'series': {'price': cls.get_series(), 'volume': cls.get_series()}}
            instruments.append(instrument)
        return instruments

    @classmethod
    def get_props(cls):
        instrument_type = random.choice(['equity', 'equity_option'])
        if instrument_type == 'equity':
            return cls.get_equity_props()
        if instrument_type == 'equity_option':
            return cls.get_equity_option_props()
        return {}

    @classmethod
    def get_equity_props(cls):
        properties = {'category': 'equity',
                      'company_name': Fake.company(),
                      'country_code': Fake.country_code()}
        return properties

    @classmethod
    def get_equity_option_props(cls):
        properties = {'category': 'equity-option',
                      'underlying_entity': Fake.company(),
                      'strike': random.expovariate(100),
                      'maturity': datetime.datetime(2035, 7, 11),
                      'option_type': random.choice(['put', 'call'])}
        return properties

    @classmethod
    def get_tickers(cls):
        tickers = [['ISIN', Fake.id(12)], ['BB_CODE', Fake.id(18)]]
        return tickers

    @classmethod
    def get_series(cls, start_year=1995):
        start_date = datetime.datetime.utcnow().replace(year=start_year, month=2, day=15)
        series = [[truncate_microseconds(start_date + datetime.timedelta(days=i)), random.expovariate(1)]
                  for i in range(cls.time_series_len)]
        series = [x for x in series if x[0] < datetime.datetime.utcnow()]
        return series


def truncate_microseconds(d: datetime.datetime):
    return d.replace(microsecond=(d.microsecond // 1000) * 1000)
