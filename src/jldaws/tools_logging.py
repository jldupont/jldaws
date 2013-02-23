"""
    Created on 2012-01-20
    @author: jldupont
"""
import os, sys, logging, hashlib
from logging.handlers import SysLogHandler
import types

PROGRESS=15


def pprint_kv(k,v, align=20):
    fmt="%-"+str(align)+"s : %s"
    print fmt % (k, v)

def info_dump(d, align):
    fmt="%-"+str(align)+"s : %s"

    if type(d)==types.DictionaryType:        
        for key in d:
            logging.info(fmt % (key, d[key]))
            
    if type(d)==types.ListType:
        for el in d:
            key, value=el
            logging.info(fmt % (key, value))
        
def setloglevel(level_name):
    """
    >>> import logging
    >>> setloglevel("info")
    >>> logging.debug("test")
    
    """
    try:
        logger=logging.getLogger()
        
        name=level_name.upper()
        if name=="PROGRESS":
            logger.setLevel(PROGRESS)
        else:
            ll=getattr(logging, name)
            logger.setLevel(ll)
    except Exception, e:
        raise Exception("Invalid log level name: %s (%s)" % (level_name, e))


class FilterDuplicates(logging.Filter):
    """
    Everything before the ':' marker is considered to be the "signature" for a log event
    
    - All DEBUG level log go through
    - All "progress" report go through
    - All messages with a ":" separator (for contextual info) are passed
    - Other messages are filtered for duplicates 
    """
    occured=[]
    
    def filter(self, record):
        if record.levelname=="DEBUG":
            return 1
        
        msg=record.getMessage()
        
        if msg.startswith("progress") or msg.startswith("Progress") or msg.startswith("PROGRESS"):
            return 1
        
        try:
            bits=msg.split(":")
            if len(bits)>1:
                return 1
            
            signature_hash=hashlib.md5(msg).hexdigest()
            if signature_hash in self.occured:
                return 0
            
            self.occured.append(signature_hash)
            record.msg="*"+msg
        except Exception,e:
            print e
            ### let it pass...
            pass
        
        return 1
        


def enable_duplicates_filter():
    logger=logging.getLogger()
    logger.addFilter(FilterDuplicates())


## ================================================================================================

def _get_fname():
    name=os.path.basename(sys.argv[0])
    cmdname=os.environ.get("CMDNAME", name)
    return "%-12s" % cmdname


FORMAT='%(asctime)s - '+_get_fname()+' - %(levelname)s - %(message)s'
     

def setup_basic_logging():    
    logging.basicConfig(level=logging.INFO, format=FORMAT)
    
    logging.addLevelName(PROGRESS, "PROGRESS")
    
    def levelProgress(self, message, *args, **kwargs):
        if self.isEnabledFor(PROGRESS):
            self._log(PROGRESS, message, args, **kwargs)
    
    logging.Logger.progress=levelProgress
    
    def progress(msg, *args, **kwargs):
        """
        Log a message with severity 'INFO' on the root logger.
        """
        logging.getLogger().progress(msg, *args, **kwargs)

    logging.progress=progress
    

def setup_syslog():

    slogger=SysLogHandler()
    
    slogger.setLevel(logging.INFO)
    formatter = logging.Formatter(FORMAT)
    slogger.setFormatter(formatter)
    
    logger=logging.getLogger()
    logger.addHandler(slogger)
    

if __name__=="__main__":
    os.environ["CMDNAME"]="test"
    setup_basic_logging()
    setup_syslog()
    logging.info("Test tools_logging...")
    
    