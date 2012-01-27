"""
    Created on 2012-01-20
    @author: jldupont
"""
import logging
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
        