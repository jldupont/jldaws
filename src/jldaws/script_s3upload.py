"""
    Created on 2012-01-20
    @author: jldupont
"""
import os, sys, logging
from time import sleep

from tools_logging import info_dump, pprint_kv
from tools_s3      import json_string
from tools_os      import resolve_path, mkdir_p, gen_walk, remove_common_prefix

def stdout(s):
    sys.stdout.write(s+"\n")


def output_json(bucket_name, prefix, keys):
    o={
       "bucket_name": bucket_name
       ,"prefix": prefix
       }
    
    
    stdout(json_string(o))


def run(args):
    
    #if args.enable_debug:
    #    logger=logging.getLogger()
    #    logger.setLevel(logging.DEBUG)
    
    bucket_name=args.bucket_name.strip()
    prefix=args.prefix
    
    num_files=args.num_files
    path_source=args.path_source
    path_moveto=args.path_moveto
    enable_delete=args.enable_delete
    enable_simulate=args.enable_simulate
    
    polling=args.polling_interval
    propagate_error=args.propagate_error
    
    always=args.always
    
    ### VALIDATE PARAMETERS
    if enable_delete and path_moveto is not None:
        raise Exception("-d can't be used with -m")
    
    code, p_src=resolve_path(path_source)
    if not code.startswith("ok"):
        raise Exception("Invalid source path: %s" % path_source)
    
    if path_moveto is not None:
        code, p_dst=resolve_path(path_moveto)
        if not code.startswith("ok"):
            raise Exception("Invalid moveto path: %s" % path_moveto)
    else:
        p_dst=None
    
    info_dump(vars(args), 20)
    
    ### wait for 'source' path to be available
    logging.info("Waiting for source path to be accessible...")
    while True:
        if os.path.isdir(p_src):
            break
        sleep(1)
    logging.info("* Source path accessible")

    logging.info("Creating moveto directory if required")
    code, _=mkdir_p(p_dst)
    if not code.startswith("ok"):
        raise Exception("Can't create 'moveto' directory: %s" % p_dst)
    logging.info("* Created moveto directory")
    
    
    logging.debug("Starting loop...")
    while True:
        logging.debug("Performing bucket 'get_all_keys'")
        #################################################
        
        try: 
            gen=gen_walk(p_src, max_files=num_files)
            for f in gen:
                process_file(f, p_src, p_dst, enable_delete, bucket_name, prefix, enable_simulate)
        except:
            logging.error("Error processing files...")


        #####################################################
        logging.debug("...sleeping for %s seconds" % polling)
        sleep(polling)
    
    
def process_file(fil, p_src, p_dst, enable_delete, bucket_name, prefix, enable_simulate):
    _, fn=remove_common_prefix(p_src, fil)
    s3key="/%s%s" % (prefix, fn)
    
    if enable_simulate:
        pprint_kv("File to be uploaded", fil)
        pprint_kv(" Filename used on s3", s3key)
        if enable_delete:
            pprint_kv(" File would be deleted", fil)
        else:
            pprint_kv(" File would be moved to", p_dst)
        

    
    
