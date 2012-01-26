"""
    Created on 2012-01-19
    @author: jldupont
"""
import time
from datetime import datetime, timedelta


def get_date():
    """
    Returns a string of the format "YYYYMMDD"
    """
    now=datetime.now()
    s="%s%s%s" % (now.year, str(now.month).zfill(2), str(now.day).zfill(2))
    return (now, s)


def get_timestamp():
    """
    Returns a string of the format "YYYYMMDDHHMMSS"
    """
    now, s=get_date()
    return (now, "%s%s%s%s" % (s, str(now.hour).zfill(2), str(now.minute).zfill(2), str(now.second).zfill(2)))


def iso_now():
    """
    YYYY-MM-DDTHH:MM:SS.xxxxxx
    
    e.g. '2012-01-25T08:58:27.514373'
    """
    now=datetime.today()
    return (now, now.isoformat())


def utc_iso_now():
    """
    Returns the present time @ GMT UTC
    
    time.struct_time(tm_year=2011, tm_mon=6, tm_mday=28, tm_hour=16, tm_min=22, tm_sec=55, tm_wday=1, tm_yday=179, tm_isdst=0)
    
    @return (time.struct_time, string)
    """
    now=time.gmtime()
    fmt="%Y-%m-%dT%H:%M:%S"
    r=time.strftime(fmt, now)
    return (now, r)

def iso_from_string(inp):
    """
    need to remove the trailing miliseconds
    
    >>> iso_from_string("2012-01-25T08:58:27.514373")
    datetime.datetime(2012, 1, 25, 8, 58, 27)
    """
    try:     r=inp.split(".")[0]
    except:  r=inp
    fmt="%Y-%m-%dT%H:%M:%S"
    return datetime.strptime(r, fmt)

def iso_string_add_delta(inp, days=0, hours=0, minutes=0):
    """
    Adds the specified amount of time to the input iso date
    >>> now, nowstr=utc_iso_now()
    >>> t, tstr=iso_string_add_delta(nowstr, days=1, hours=12)
    >>> print t.day-now.tm_mday
    1
    >>> print t.hour-now.tm_hour
    12
    """
    dt=iso_from_string(inp)
    td=timedelta(days=days, hours=hours, minutes=minutes)
    r=dt+td
    return (r, r.isoformat())


def iso_now_and_then(days=0, hours=0, minutes=0):
    """
    return e.g. ('2012-01-25T13:58:27', '2012-01-27T01:58:27')
    
    >>> now, then=iso_now_and_then(days=1, hours=12)
    >>> r=iso_delta_from_strings(now, then)
    >>> print r.days
    1
    >>> print r.seconds
    43200
    """
    _, nowstr=utc_iso_now()
    _, thenstr=iso_string_add_delta(nowstr, days=days, hours=hours, minutes=minutes)
    return (nowstr, thenstr)
    


def iso_delta_from_strings(t1, t2):
    """
    Computes the delta (in seconds) between t1 and t2
    
    >>> import time
    >>> _, t1=utc_iso_now()
    >>> time.sleep(2)
    >>> _, t2=utc_iso_now()
    >>> delta=iso_delta_from_strings(t1, t2)
    >>> print delta.seconds
    2
    >>> print delta.days
    0
    
    @return datetime.timedelta
    """
    t1c=iso_from_string(t1)
    t2c=iso_from_string(t2)
    return t2c-t1c


if __name__=="__main__":
    import doctest
    doctest.testmod()