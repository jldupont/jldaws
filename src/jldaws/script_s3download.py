"""
    Created on 2012-01-20
    @author: jldupont
"""
import sys, logging
from time import sleep

from tools_s3    import json_string, get_all_keys
from tools_os    import resolve_path, safe_path_exists
from tools_os    import can_write, check_can_write_paths, check_exist_paths
from tools_sys   import retry
from tools_func  import transition_manager
import boto
from boto.s3.key import Key as S3Key

from pyfnc import patterned, pattern, partial, dic, coroutine

def stdout(s):
    sys.stdout.write(s+"\n")


def log(msg, path):
    logging.info(msg % path)
    


def run(bucket_name=None, prefix=None,
        path_dest=None, path_check=None,
        dont_dl=None, just_keys=None,
        num_files=5, propagate_error=False,  polling_interval=None):
    
    if path_check is not None:
        code, path_check=resolve_path(path_check)
        if not code.startswith("ok"):
            logging.warning("path_check '%s' might be in error..." % path_check)
    
    ### VALIDATE PARAMETERS
    
    code, path_dest=resolve_path(path_dest)
    if not code.startswith("ok"):
        raise Exception("Invalid destination path: %s" % path_dest)
    
    code, _path=can_write(path_dest)
    if not code.startswith("ok"):
        raise Exception("Can't write to destination path: %s" % path_dest)
    
    logging.info("* Can write to destination path")
    
    try:
        conn = boto.connect_s3()
    except:
        ## not much we can do
        ## but actually no remote calls are made
        ## at this point so it should be highly improbable
        raise Exception("Can't 'connect' to S3")
    
    ###################### BUCKET
    logging.info("Getting/creating bucket (unlimited retries with backoff)")
    def _get_create_bucket():
        return conn.create_bucket(bucket_name)
              
    bucket=retry(_get_create_bucket)
    logging.info("Got bucket")
    #############################

    

    ctx=dic({
         "bucket":     bucket
         ,"prefix":    prefix
         ,"path_dest": path_dest
         ,"just_keys": just_keys
         ,"dont_dl":   dont_dl
         ,"num_files": num_files
         ,"cp": {
                 "up":    partial(log, "Check path OK:  %s", path_check)
                 ,"down": partial(log, "Check path !OK: %s", path_check) 
                 }
         })

    ctx["tm"]=transition_manager(ctx)
    ctx["mk"]=manager_keys(ctx)
    
    
    logging.info("Starting loop...")
    
    while True:
        
        #################################################
        try:
            _code, path_exists=safe_path_exists(path_check)            
            maybe_process(ctx, path_check, path_exists)
            
        except Exception, e:
            logging.error(str(e))

        #####################################################
        logging.debug("...sleeping for %s seconds" % polling_interval)
        sleep(polling_interval)
    
    
@pattern(dic, None, any)
def maybe_process_1(ctx, _1, _2):
    process(ctx)

@pattern(dic, any, False)
def maybe_process_2(ctx, _1, _2):
    """ nothing to do has check_path doesn't exist """
    ctx["tm"].send(("cp", False))
    
@pattern(dic, any, True)
def maybe_process_3(ctx, _1, _2):
    ctx["tm"].send(("cp", True))
    process(ctx)

@patterned
def maybe_process(ctx, path_check, path_check_exists): pass


def process(ctx):
    """
    Really process from here :)
    """
    bucket=ctx["bucket"]
    prefix=ctx["prefix"]
    path_dest=ctx["path_dest"]
    
    code, maybe_keys=get_all_keys(bucket, prefix)
    if not code.startswith("ok"):
        e=maybe_keys
        logging.warning("Error retrieving keys: %s" % str(e))
        return

    ## Check if it already exists 
    results_exists=check_exist_paths(path_dest, maybe_keys)

    def remove_exists(entry):
        code, (_path_fragment, exists)=entry
        if not code.startswith("ok"):
            return False ## filter-out
        return not exists
    
    potential_todo=filter(remove_exists, results_exists)

    def keep_only_keys(entry):
        _, (key, state)=entry
        return key
    
    potential_todo_keys=map(keep_only_keys, potential_todo)

    ## Check if 'key' can be written to
    results=check_can_write_paths(path_dest, potential_todo_keys)
    
    def keep(which):
        
        def switch(status):
            return status if which=="writable" else not status
        
        def _(entry):
            code, (_fragment, status)=entry
            if not code.startswith("ok"):
                return switch(False)
            
            return switch(status)
        return _
            
    good_key_entries=filter(keep("writable"), results)
    bad_key_entries=filter(keep("!writable"), results)

    ## Report bad keys to manager_keys
    ctx["mk"].send(bad_key_entries)
        
    process2(ctx, ctx["just_keys"], good_key_entries)
    
    
@pattern(dic, True, list)
def process2_justkeys(ctx, _, keys):
    """
    """
    print keys
    
@pattern(dic, False, list)
def process2_dl(ctx, _, keys):
    """
    """
    print keys
    
@patterned
def process2(ctx, just_keys, keys): pass


    

@coroutine
def manager_keys(ctx):
    
    while True:
        entries=(yield)
        


        
        
