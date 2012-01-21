"""
    Created on 2012-01-20
    @author: jldupont
"""
import sys
import logging
from time import sleep

from tools_logging import info_dump
from tools_mod     import call
from tools_s3      import keys_to_dict, check_changes, bucket_status, json_string, key_object_to_dict

def stdout(s):
    sys.stdout.write(s+"\n")


def output_json(bucket_name, prefix, keys, changes):
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
    
    bucket_name=args.bucket_name
    module_name=args.module_name
    polling=args.polling_interval
    enable_json=args.enable_json
    prefix=args.prefix
    always=args.always

    enable_module_send=False if module_name.lower()=="none" else True

    info_dump(args._get_kwargs(), 20)
    
    logging.debug("Starting loop...")
    while True:
        logging.debug("Performing bucket 'get_all_keys'")
                
        try:
            code, data=bucket_status(bucket_name, prefix)
        except Exception,e:
            logging.error("Can't connect to S3")
            code="error"
            
        if code.startswith("ok"):
                       
            try:
                ndict=keys_to_dict(data)
                changes=check_changes(cdict, ndict)
                if len(changes)>0 or always:
                    try:
                        if enable_module_send:
                            call(module_name, "run", bucket_name, prefix, keys=ndict, changes=changes)
                            logging.debug("success making the 'run' call")                        
                    except Exception, e:
                        logging.error("calling 'run' function of module '%s': %s" %(module_name, str(e)))
                    
                    cdict=ndict        
                    try:
                        if enable_json:
                            output_json(bucket_name, prefix, data, changes)
                            logging.debug("success with output of json to stdout")
                            
                    except Exception, e:
                        logging.error("generating json output to stdout: %s" % str(e))
            except:
                logging.error("checking changes in bucket '%s', prefix '%s'" % (bucket_name, prefix))
        
        logging.debug("...sleeping for %s seconds" % polling)
        sleep(polling)
    
    
