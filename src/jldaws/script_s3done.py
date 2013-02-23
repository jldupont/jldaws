"""
    Created on 2012-01-20
    @author: jldupont
"""
import os, sys, logging
from time import sleep

from tools_s3      import json_string
from tools_os      import resolve_path, mkdir_p, get_root_dirs
from tools_os      import rmdir
from tools_sys     import retry, move

import boto
from boto.s3.key import Key as S3Key

def stdout(s):
    sys.stdout.write(s+"\n")

def report(ctx, ctx2):
    d=dict(ctx2)
    d.update(ctx)
    stdout(json_string(d)) 


def run(bucket_name=None, 
        path_source=None, 
        path_move=None,
        delete_source=False,
        polling_interval=60,
        extd=None,
        extf=None
        ,**_):
        

    if not delete_source and path_move is None:
        raise Exception("Options 'delete source' or 'move path' is required")
    
    if delete_source and path_move is not None:
        raise Exception("Options 'delete source' and 'move path' are mutually exclusive")
    
    
    #if args.enable_debug:
    #    logger=logging.getLogger()
    #    logger.setLevel(logging.DEBUG)
    
    bucket_name=bucket_name.strip()
    path_source=path_source.strip()
    
    code, p_src=resolve_path(path_source)
    if not code.startswith("ok"):
        raise Exception("Invalid source path: %s" % path_source)

    mkdir_p(p_src)

    if path_move is not None:
        code, path_move=resolve_path(path_move)
        if not code.startswith("ok"):
            raise Exception("Invalid move path: %s" % path_move)
    
        code,_=mkdir_p(path_move)
        if not code.startswith("ok"):
            raise Exception("Can't create move path: %s" % path_move)
    
    
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
    logging.info("Got bucket: %s" % bucket_name)
    #############################

    logging.debug("Starting loop...")

    ppid=os.getppid()
    logging.info("Process pid: %s" % os.getpid())
    logging.info("Parent pid:  %s" % ppid)
    while True:
        if os.getppid()!=ppid:
            logging.warning("Parent terminated... exiting")
            break
        #################################################

        logging.debug("Start processing...")
        
        code, dirs=get_root_dirs(p_src)
        if not code.startswith("ok"):
            raise Warning("Source path disappeared: %s" % p_src)
        
        dirs=filter_dirs(extd, dirs)
        
        for _dir in dirs:
            process_dir(bucket, _dir, delete_source, extf, path_move)
        

        #####################################################
        logging.debug("...sleeping for %s seconds" % polling_interval)
        sleep(polling_interval)


def filter_dirs(extd, dirs):
    
    def f(d):
        return d.endswith(extd)
    
    return filter(f, dirs)


def process_dir(bucket, _dir, delete_source, extf, path_move):
    
    logging.progress("Processing: %s" % _dir)
    
    parts=os.path.splitext(_dir)
    bn=os.path.basename(parts[0])
    filename="%s.%s" % (bn, extf)
    
    logging.progress("Generating: file '%s' in bucket '%s'" % (filename, bucket.name))

    k=S3Key(bucket)
    k.key=filename
    
    try:
        k.set_contents_from_string("")
    except:
        logging.warning("Can't generate file '%s' in bucket '%s'" % (filename, bucket.name))
        return
    
    if delete_source:
        logging.progress("Deleting: %s" % _dir)
        rmdir(_dir)
        return
    
    if path_move is not None:
        
        bn_src_dir=os.path.basename(_dir)
        ddir=os.path.join(path_move, bn_src_dir)
        rmdir(ddir)
        
        logging.progress("Moving: %s => %s" % (_dir, ddir))
        code, msg=move(_dir, ddir)
        if not code.startswith("ok"):
            logging.warning("Can't move '%s' to '%s': %s" % (_dir, ddir, msg))

    
            

    