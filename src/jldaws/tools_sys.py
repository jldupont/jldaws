"""
    Created on 2012-01-26
    @author: jldupont
"""
import os, shutil
from time import sleep

try:    import json
except: json=None
    
try:    import yaml
except: yaml=None

from jldaws.tools_os import resolve_path, file_contents


def move(src_path, dst_path):
    try:
        shutil.move(src_path, dst_path)
        return ("ok", None)
    except Exception, e:
        return ("error", e)


def get_cfg(path_cfg_file):
    """
    Get configuration dictionary from a filesystem path    
    """
    path=resolve_path(path_cfg_file)
    _root, ext=os.path.splitext(path)
    
    if ext!="json" and ext!="yaml":
        return ("error", "unsupported file format")

    code, maybe_data=file_contents(path)
    if not code.startswith("ok"):
        return ("error", "reading file '%s'" % path)
    
    if ext=="json" and json is None:
        return ("error", "JSON parser not available")
    
    if ext=="yaml" and yaml is None:
        return ("error", "YAML parser not available")
    
    if ext=="json":
        try:
            data=json.loads(maybe_data)
        except: 
            return ("error", "JSON parsing")
    
    if ext=="yaml":
        try:    
            data=yaml.load(maybe_data)
        except: 
            return ("error", "YAML parsing")
    
    return ("ok", data)


def retry(f, always=True, min_wait=1, max_wait=30, max_retries=10):
    """
    Retries function 'f' : the function should throw an exception to indicate failure 
    
    :param always: boolean, should always retry true/false
    :param min_wait: int, minimum seconds between tries
    :param max_wait: int, maximum seconds before restarting cycle
    :param max_retries: int, maximum number of retries when 'always==False'
    
    @raise KeyboardInterrupt
    @return f()
    """
    
    wait=min_wait
    retry_count=max_retries
    while True:
        try:
            return f()
        except KeyboardInterrupt:
            raise
        except Exception, exp:   
                         
            retry_count=max(retry_count-1, 0)
            if not always and retry_count==0:
                raise exp
            
        sleep(wait)
        
        wait=wait*2
        wait=wait if wait<max_wait else min_wait



if __name__=="__main__":
    import doctest
    doctest.testmod()
