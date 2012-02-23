"""
    Created on 2012-01-20
    @author: jldupont
"""
import sys, os
import logging
import copy
from time import sleep

from tools_logging import info_dump
from tools_mod     import call
from tools_s3      import keys_to_dict, compute_changes, bucket_status, json_string, key_object_to_dict

def stdout(s):
    sys.stdout.write(s+"\n")


def output_json(bucket_name, prefix, keys, changes, additions, substractions):
    o={
       "bucket_name": bucket_name
       ,"prefix": prefix
       }
    
    rkeys={}
    for key in keys:
        ko=key_object_to_dict(key)
        rkeys.update(ko)
        
    o["keys"]=rkeys
    o["changes"]=changes
    o["additions"]=additions
    o["substractions"]=substractions
    
    stdout(json_string(o))


def run(args):
    """
    args.bucket_name
    args.module_name
    """
    cdict={}
    
    if args.enable_debug:
        logger=logging.getLogger()
        logger.setLevel(logging.DEBUG)
    
    bucket_name=args.bucket_name.strip()
    module_name=args.module_name
    polling=args.polling_interval
    enable_json=args.enable_json
    propagate_error=args.propagate_error
    prefix=args.prefix
    always=args.always

    if module_name is not None:
        module_name=module_name.strip()
        
    info_dump(args._get_kwargs(), 20)
    
    ppid=os.getppid()
    logging.info("Process pid: %s" % os.getpid())
    logging.info("Parent pid: %s" % ppid)
    logging.info("Starting loop...")
    while True:
        if os.getppid()!=ppid:
            logging.warning("Parent terminated... exiting")
            break

        logging.debug("Performing bucket 'get_all_keys'")
                
        try:
            code, data=bucket_status(bucket_name, prefix)
        except Exception,e:
            logging.error("Can't reach S3")
            code="error"
            if propagate_error:
                stdout('''{"error": "Can't reach S3"}''')
            
        if code.startswith("ok"):
            try:
                ndict=keys_to_dict(data)
                changes, additions, substractions=compute_changes(cdict, ndict)
                
                if len(changes)>0 or len(additions)>0 or len(substractions)>0 or always:
                    try:
                        if module_name is not None:
                            call(module_name, "run", bucket_name, prefix, keys=ndict, changes=changes, additions=additions, substractions=substractions)
                            logging.debug("success making the 'run' call")                        
                    except Exception, e:
                        logging.error("calling 'run' function of module '%s': %s" %(module_name, str(e)))
                    
                            
                    try:
                        if enable_json:
                            output_json(bucket_name, prefix, data, changes, additions, substractions)
                            logging.debug("success with output of json to stdout")
                            
                    except Exception, e:
                        logging.error("generating json output to stdout: %s" % str(e))
                        
                cdict=dict(ndict)
                
            except:
                logging.error("checking changes in bucket '%s', prefix '%s'" % (bucket_name, prefix))
        
        logging.debug("...sleeping for %s seconds" % polling)
        sleep(polling)
    
    
