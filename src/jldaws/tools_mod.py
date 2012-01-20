"""
    Various module level functions
    
    Created on 2012-01-20
    @author: jldupont
"""
import importlib

def _echo(st):
    return st


def call(module_name, function_name, *pargs, **kargs):
    """
    >>> call("tools_mod", "_echo", "hello!")
    'hello!'
    """
    try:
        mod=importlib.import_module(module_name)
    except:
        raise Exception("Can't import module '%s'" % module_name)
        
    try:
        fnc=getattr(mod, function_name)
    except:
        raise Exception("Module '%s' doesn't have a 'run' function" % mod)
    
    if not callable(fnc):
        raise Exception("Can't call 'run' function of callable '%S'" % module_name)
    
    return fnc(*pargs, **kargs)
    

if __name__=="__main__":
    import doctest
    doctest.testmod()
    