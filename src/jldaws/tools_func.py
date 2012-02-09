"""
    Created on 2012-02-08
    @author: jldupont
"""
from pyfnc import patterned, pattern, coroutine


@pattern(None, None)
def is_trans_NN(previous, current):
    return ('nop', None)

@pattern(None, bool)
def is_trans_NB(previous, current):
    return ('tr', "up" if current is True else "down")
    
@pattern(False, False)
def is_trans_FF(previous, current):
    return ("nop", None)

@pattern(True, True)
def is_trans_TT(previous, current):
    return ("nop", None)
    
@pattern(True, False)
def is_trans_TF(previous, current):
    return ("tr", "down")

@pattern(False, True)
def is_trans_FT(previous, current):
    return ("tr", "up")
    
@patterned
def is_trans(previous, current):
    """
    >>> is_trans(True, True)
    ('nop', None)
    >>> is_trans(False, False)
    ('nop', None)
    >>> is_trans(True, False)
    ('tr', 'down')
    >>> is_trans(False, True)
    ('tr', 'up')
    >>> is_trans(None, None)
    ('nop', None)
    >>> is_trans(None, True)
    ('tr', 'up')
    >>> is_trans(None, False)
    ('tr', 'down')
    """

@coroutine
def transition_manager(ctx):
    """
    ctx[param]={ "state": state, "up": fnc, "down": fnc}
    
    >>> import logging
    >>> fu=lambda:logging.warning("up")
    >>> fd=lambda:logging.warning("down")
    >>> ctx={"test":{"up": fu, "down":fd}}
    >>> tm=transition_manager(ctx)
    >>> tm.send(("test", True))   ### warning output 'up'
    >>> tm.send(("test", True))
    >>> tm.send(("test", False))  ### warning output 'down'
    >>> tm.send(("test", False))
    """    
    while True:
        param, current_state=(yield)
        cparam=ctx.get(param, {})
        
        previous_state=cparam.get("previous", None)
        maybe_tr, maybe_dir=is_trans(previous_state, current_state)
        cparam["previous"]=current_state
        ctx[param]=cparam
        
        if maybe_tr=="tr":
            fnc=cparam.get(maybe_dir, None)
            if fnc is not None:
                fnc()
        
        
if __name__=="__main__":
    import doctest
    doctest.testmod()
