"""
    Created on 2012-01-19
    @author: jldupont
"""
import uuid

from tools_date import get_timestamp

def gen_queue_name():
    """
    >>> _gen_queue_name()
    """
    _, ts=get_timestamp()
    queue_name="ex_%s_%s" % (ts, uuid.uuid1())
    return queue_name

if __name__=="__main__":
    import doctest
    doctest.testmod()
