import datetime


def rfc3339_datetime_to_str(d: datetime.datetime):
    return f'{d.year}-{d.month:02}-{d.day:02}T{d.hour:02}:{d.minute:02}:{d.second:02}.{d.microsecond}Z'


def rfc3339_str_to_datetime(s: str):
    d = datetime.datetime.utcnow().replace(tzinfo=None)
    if len(s) < 20:
        raise ValueError(f'{s} is not an RFC3339 datetime.')
    if s[-1] != 'Z' or s[10] != 'T':
        raise ValueError(f'{s} is not an RFC3339 datetime.')
    try:
        d.replace(year=int(s[0:4]))
        d.replace(month=int(s[5:7]))
        d.replace(day=int(s[8:10]))
        d.replace(hour=int(s[11:13]))
        d.replace(minute=int(s[14:16]))
        d.replace(second=int(s[17:19]))
        if s[19] == '.' and len(s) > 21:
            ms_str = s[20:-1]
            d.replace(microsecond=int(ms_str)*(10**(6-len(ms_str))))
        else:
            d.replace(microsecond=0)
    except ValueError:
        raise ValueError(f'{s} is not an RFC3339 datetime.')
    return d
