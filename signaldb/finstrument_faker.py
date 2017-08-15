import datetime
import random
import faker


class InstrumentFaker:
    """Random financial instrument generator"""
    fake = faker.Faker()
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
                      'company_name': cls.fake.company(),
                      'country_code': cls.fake.country_code()}
        return properties

    @classmethod
    def get_equity_option_props(cls):
        properties = {'category': 'equity-option',
                      'underlying_entity': cls.fake.company(),
                      'strike': random.expovariate(100),
                      'maturity': cls.fake.date_time_this_century(after_now=True),
                      'option_type': random.choice(['put', 'call'])}
        return properties

    @classmethod
    def get_tickers(cls):
        tickers = [['ISIN', cls.fake.md5().upper()[:12]], ['BB_CODE', cls.fake.md5().upper()]]
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
