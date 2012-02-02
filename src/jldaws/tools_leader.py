"""
    Created on 2012-02-01
    @author: jldupont
"""

from tools_sys import coroutine

def _test_leader_tracking():
    """
    ... because I can't put the doctest in the function declaration itself: http://bugs.python.org/issue6835
    
    >>> lt=leader_tracking()
    >>> print lt.send(False)
    ('nop', None)
    >>> print lt.send(False)
    ('nop', None)
    >>> print lt.send(True)
    ('tr', 'up')
    >>> print lt.send(True)
    ('nop', None)
    >>> print lt.send(False)
    ('tr', 'down')
    >>> print lt.send(False)
    ('nop', None)
    """

@coroutine
def leader_tracking():
    """
    Tracks 'leader' status - only returns transitions
    """
    last_status=False
    result=("nop", None)
    
    while True:
        status=(yield result)
        
        if status!=last_status:
            direction="up" if last_status==False else "down"
            last_status=status
            result=("tr", direction)
        else:
            result=("nop", None)

if __name__=="__main__":
    import doctest
    doctest.testmod()
    