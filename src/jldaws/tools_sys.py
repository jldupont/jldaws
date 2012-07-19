"""
    Created on 2012-01-26
    @author: jldupont
"""
import shutil, sys, json, logging
from time import sleep
import functools

from tools_logging import enable_duplicates_filter, setloglevel, info_dump, setup_syslog

class SignalTerminate(Exception): pass

def process_command_line(parser):
    parser.add_argument('-lc',  dest="logconfig", type=str,  help="Logging configuration file", choices=["debug", "info", "warning", "error"])
    parser.add_argument('-ll',  dest='log_level',     type=str,            help="Log Level", default="info", choices=["debug", "info", "warning", "error"])
    parser.add_argument('-kdf', dest='log_keepdup',   action="store_true", help="Keep duplicate log entries", default=False)
    parser.add_argument('-lsl', dest='log_syslog',    action="store_true", help="Enable syslog", default=False)
    
    args=dnorm(vars(parser.parse_args()))
    
    if (args["log_syslog"]):
        setup_syslog()
        
    if (not args["log_keepdup"]):
        enable_duplicates_filter()
        
    setloglevel(args["log_level"])
    info_dump(args, 20)
    return args


def coroutine(func):
    @functools.wraps(func)
    def start(*args, **kwargs):
        cr=func(*args, **kwargs)
        cr.next()
        return cr
    return start

def jstdout(jo):
    sys.stdout.write(json.dumps(jo)+"\n")
    sys.stdout.flush()

def stdout(s):
    sys.stdout.write(s+"\n")
    sys.stdout.flush()
    
def dnorm(d):
    """
    Normalize dictionary
    
    >>> dnorm({"SoMeKeY":"  spaces  "})
    {'somekey': 'spaces'}
    """
    r={}
    for e in d:
        try:    r[e.lower()]=d[e].strip()
        except: r[e.lower()]=d[e]
    return r


def dstrip(d):
    """
    Strip each element in a dict
    
    >>> dstrip({"e1":" v1", "e2":" v2 "})
    {'e1': 'v1', 'e2': 'v2'}
    """
    for e in d:
        try:    d[e]=d[e].strip()
        except: pass
    return d


def move(src_path, dst_path):
    try:
        shutil.move(src_path, dst_path)
        return ("ok", None)
    except Exception, e:
        return ("error", e)


def retry(f, always=True, min_wait=1, max_wait=30, max_retries=10, logmsg=None):
    """
    Retries function 'f' : the function should throw an exception to indicate failure 
    
    :param always: boolean, should always retry true/false
    :param min_wait: int, minimum seconds between tries
    :param max_wait: int, maximum seconds before restarting cycle
    :param max_retries: int, maximum number of retries when 'always==False'
    
    @raise KeyboardInterrupt
    @return f()
    """
    showed_msg=False
    wait=min_wait
    retry_count=max_retries
    while True:
        try:
            return f()
        except KeyboardInterrupt:
            raise
        except Exception, exp:   
            if not showed_msg:
                showed_msg=True
                if logmsg is not None:
                    logging.warning(logmsg)     
            retry_count=max(retry_count-1, 0)
            if not always and retry_count==0:
                raise exp
            
        sleep(wait)
        
        wait=wait*2
        wait=wait if wait<max_wait else min_wait



if __name__=="__main__":
    import doctest
    doctest.testmod()
