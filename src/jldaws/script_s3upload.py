"""
    Created on 2012-01-20
    @author: jldupont
"""
import os, sys, logging
from time import sleep

from tools_logging import info_dump, pprint_kv
from tools_s3      import json_string
from tools_os      import resolve_path, mkdir_p, gen_walk, remove_common_prefix
from tools_os      import can_write, rm
from tools_sys     import retry, move

import boto
from boto.s3.key import Key as S3Key

def stdout(s):
    sys.stdout.write(s+"\n")

def report(ctx, ctx2):
    d=dict(ctx2)
    d.update(ctx)
    stdout(json_string(d)) 


def run(args):
    
    #if args.enable_debug:
    #    logger=logging.getLogger()
    #    logger.setLevel(logging.DEBUG)
    
    bucket_name=args.bucket_name.strip()
    prefix=args.prefix.strip()
    
    num_files=args.num_files
    path_source=args.path_source.strip()
    
    try:    path_moveto=args.path_moveto.strip()
    except: path_moveto=None
    
    enable_delete=args.enable_delete
    enable_simulate=args.enable_simulate
    
    polling=args.polling_interval
    propagate_error=args.propagate_error
    
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

    if path_moveto is not None:
        logging.info("Creating moveto directory if required")
        code, _=mkdir_p(p_dst)
        if not code.startswith("ok"):
            raise Exception("Can't create 'moveto' directory: %s" % p_dst)
        logging.info("* Created moveto directory")
    
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

    if enable_simulate:
        logging.info("Begin simulation...")
    else:
        logging.debug("Starting loop...")
    while True:
        #################################################
        
        try: 
            gen=gen_walk(p_src, max_files=num_files)
            for src_filename in gen:
                
                logging.info("Processing file: %s" % src_filename)                
                s3key_name=gen_s3_key(p_src, src_filename, prefix)
                
                if enable_simulate:
                    simulate(src_filename, s3key_name, enable_delete, p_dst)
                else:
                    k=S3Key(bucket)
                    k.key=s3key_name
                    process_file(bucket_name, prefix, k, src_filename, p_dst, enable_delete, propagate_error)

        except Exception, e:
            logging.error("Error processing files...(%s)" % str(e))


        #####################################################
        logging.debug("...sleeping for %s seconds" % polling)
        sleep(polling)
    

def gen_s3_key(p_src, filename, prefix):
    _, fn=remove_common_prefix(p_src, filename)
    return "/%s%s" % (prefix, fn)
    
    
def simulate(fil, s3key_name, enable_delete, p_dst):
    pprint_kv("File to be uploaded", fil)
    pprint_kv(" Filename used on s3", s3key_name)
    
    if enable_delete:
        _c, f_is_writable=can_write(fil)        
        pprint_kv(" File would be deleted", fil)
        if not f_is_writable:
            pprint_kv(" ! File can't be deleted", fil)
    else:
        bname=os.path.basename(fil)
        fdst=os.path.join(p_dst, bname)           
        _c, d_is_writable=can_write(p_dst)            
        pprint_kv(" File would be moved to", fdst)
        if not d_is_writable:
            pprint_kv(" ! File can't be moved to", p_dst)


MSGS={
      "up":    "Uploading to S3"
      ,"del":  "Deleting source file"
      ,"move": "Moving source file"
      }    
def process_file(bucket_name, prefix, k, src_filename, p_dst, enable_delete, propagate_error):
    
    ctx={"src": src_filename, "key": k.name, "bucket": bucket_name, "prefix": prefix}
    
    #1) Upload to S3
    try:
        k.set_contents_from_filename(src_filename)
        report(ctx, {"code":"ok", "kind":"upload"})
    except:
        msg=MSGS["up"] % src_filename
        if propagate_error:
            report(ctx, {"code":"error", "kind":"upload"})
        return
    
    #2a) Delete
    if enable_delete:
        code, msg=rm(src_filename)
        if not code.startswith("ok"):
            logging.debug("Error deleting: %s (%s)" % (src_filename, msg))
            if not propagate_error:
                return

        report(ctx, {"code": code, "kind":"delete"})
            
    #2b) Move
    else:
        bname=os.path.basename(src_filename)
        dst_filename=os.path.join(p_dst, bname)
        code, msg=move(src_filename, dst_filename)
        if not code.startswith("ok"):
            ## 1 last chance... try recreating the dst directory for next run...
            mkdir_p(p_dst)
            logging.debug("Error moving: %s ==> %s  (%s)" % (src_filename, dst_filename, msg))
            if not propagate_error:
                return

        report(ctx, {"code":code, "kind":"move", "dst":dst_filename})
    

