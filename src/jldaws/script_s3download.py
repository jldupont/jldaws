"""
    Created on 2012-01-20
    @author: jldupont
"""
import sys, logging
from time import sleep

from tools_s3    import json_string, get_all_keys
from tools_os    import resolve_path, safe_path_exists
from tools_os    import can_write
from tools_sys   import retry
from tools_func  import transition_manager
import boto
from boto.s3.key import Key as S3Key

from pyfnc import patterned, pattern, partial

def stdout(s):
    sys.stdout.write(s+"\n")


def log(msg, path):
    logging.info(msg % path)
    


def run(bucket_name=None, prefix=None,
        path_dest=None, path_check=None,
        dont_dl=None, just_keys=None,
        num_files=5, propagate_error=False,  polling_interval=None):
    
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

    ctx={
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
         }

    ctx["tm"]=transition_manager(ctx)
    
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
    
    
@pattern(dict, None, any)
def maybe_process_1(ctx, _1, _2):
    process(ctx)

@pattern(dict, any, False)
def maybe_process_2(ctx, _1, _2):
    """ nothing to do has check_path doesn't exist """
    tm=ctx["tm"]
    tm.send(("cp", False))
    
@pattern(dict, any, True)
def maybe_process_3(ctx, _1, _2):
    tm=ctx["tm"]
    tm.send(("cp", True))
    process(ctx)

@patterned
def maybe_process(ctx, path_check, path_check_exists): pass


def process(ctx):
    """
    Really process from here :)
    """
    bucket=ctx["bucket"]
    prefix=ctx["prefix"]
    
    code, maybe_keys=get_all_keys(bucket, prefix)
    if not code.startswith("ok"):
        e=maybe_keys
        logging.warning("Error retrieving keys: %s" % str(e))
        return
    
    process2(ctx, ctx["just_keys"], maybe_keys)
    
    
@pattern(dict, True, list)
def process2_justkeys(ctx, _, keys):
    print keys
    
@pattern(dict, False, list)
def process2_dl(ctx, _, keys):
    print "all!"
    
@patterned
def process2(ctx, just_keys, keys): pass


