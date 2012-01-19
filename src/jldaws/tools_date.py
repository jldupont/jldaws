"""
    Created on 2012-01-19
    @author: jldupont
"""

import datetime


def get_date():
    """
    Returns a string of the format "YYYYMMDD"
    """
    now=datetime.datetime.now()
    s="%s%s%s" % (now.year, str(now.month).zfill(2), str(now.day).zfill(2))
    return (now, s)


def get_timestamp():
    """
    Returns a string of the format "YYYYMMDDHHMMSS"
    >>> get_timestamp()
    
    """
    now, s=get_date()
    return (now, "%s%s%s%s" % (s, str(now.hour).zfill(2), str(now.minute).zfill(2), str(now.second).zfill(2)))

if __name__=="__main__":
    import doctest
    doctest.testmod()