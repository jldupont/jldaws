"""
    Created on 2012-01-20
    @author: jldupont
"""
import logging, hashlib
import types

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
        ll=getattr(logging, level_name.upper())
        logger=logging.getLogger()
        logger.setLevel(ll)
    except:
        raise Exception("Invalid log level name: %s" % level_name)


class FilterDuplicates(logging.Filter):
    """
    Everything before the ':' marker is considered to be the "signature" for a log event
    """
    occured=[]
    
    def filter(self, record):
        try:
            bits=record.getMessage().split(":")
            signature_hash=hashlib.md5(bits[0]).hexdigest()
            if signature_hash in self.occured:
                return 0
            self.occured.append(signature_hash)
        except:
            ### let it pass...
            pass
        
        return 1
        


def enable_duplicates_filter():
    logger=logging.getLogger()
    logger.addFilter(FilterDuplicates())
