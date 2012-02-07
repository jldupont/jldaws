"""
    Created on 2012-02-01
    @author: jldupont
"""
from tools_sys import coroutine

@coroutine
def leader_tracking():
    """
    Tracks 'leader' status - only returns transitions
    
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


@coroutine
def protocol_processor(node_id, param_n, param_m):
    """
    >>> pp=protocol_processor(10, 2, 3)
    >>> pp.send((0, 11))
    (False, 'NL')
    >>> pp.send((0, 12))
    (False, 'NL')
    >>> pp.send((0, 13))
    (False, 'NL')
    >>> pp.send((1, 11))
    (False, 'NL')
    >>> pp.send((1, 12))
    (False, 'NL')
    >>> pp.send((1, 13))
    (False, 'NL')
    >>> pp.send((2, 11))
    (True, 'ML')
    >>> pp.send((2, 12))
    (False, 'ML')
    >>> pp.send((2, 13))
    (False, 'ML')
    >>> pp.send((3, 11))
    (True, 'L')
    >>> pp.send((3, 12))
    (False, 'L')
    >>> pp.send((4, 9))
    (True, 'NL')
    """
    sp=state_processor(param_n, param_m)
    
    tr=False
    current_state="NL"
    current_poll_count=0
    current_poll_winning=True
    
    while True:
        poll_count, advertised_nodeid=(yield (tr, current_state))
        tr=False
        
        ## latch on bad news...
        ## i.e. only 1 lost interval crashes the interval
        if current_poll_winning:
            current_poll_winning=(advertised_nodeid>=node_id)
        
        if current_poll_count!=poll_count:
            current_poll_count=poll_count
            tr, current_state=sp.send(current_poll_winning)
            current_poll_winning=True

@coroutine
def state_processor(param_n, param_m):
    
    last_state="NL"
    consecutive_winning_intervals=0
    current_state="NL"
    while True:

        tr=last_state != current_state
        last_state=current_state
            
        won_interval=(yield (tr, current_state))
        
        ## loose leadership easily :(
        if not won_interval:
            consecutive_winning_intervals=0
            current_state="NL"
            continue
        
        ### won this interval:    
        consecutive_winning_intervals=consecutive_winning_intervals+1

        if current_state=="NL":
            if consecutive_winning_intervals >= param_n:
                current_state="ML"
                continue
        
        if current_state=="ML":
            if consecutive_winning_intervals >= param_m:
                current_state="L"
                consecutive_winning_intervals=0
                continue
            


if __name__=="__main__":
    import doctest
    doctest.testmod()
    