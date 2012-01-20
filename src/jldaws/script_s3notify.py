"""
    Created on 2012-01-20
    @author: jldupont
"""
import logging
from time import sleep

from tools_logging import info_dump
from tools_mod     import call
from tools_s3      import keys_to_dict, check_changes, bucket_status

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
    prefix=args.prefix
    always=args.always

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
                try:
                    if len(changes)>0 or always:
                        call(module_name, "run", bucket_name, prefix, keys=ndict, changes=changes)
                        cdict=ndict
                        logging.debug("success making the 'run' call")
                except Exception, e:
                    logging.error("calling 'run' function of module '%s': %s" %(module_name, str(e)))
            except:
                logging.error("checking changes in bucket '%s', prefix '%s'" % (bucket_name, prefix))
        
        logging.debug("...sleeping for %s seconds" % polling)
        sleep(polling)
    
    
