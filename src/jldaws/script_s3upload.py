"""
    Created on 2012-01-20
    @author: jldupont
"""
import os, sys, logging, re
from time import sleep

from tools_logging import pprint_kv
from tools_s3      import json_string
from tools_os      import resolve_path, mkdir_p, gen_walk, remove_common_prefix, safe_path_exists, touch
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


def run(enable_simulate=False, bucket_name=None, 
        path_source=None, path_moveto=None, path_check=None,
        num_files=5, enable_delete=False, propagate_error=False, prefix=None, polling_interval=None
        ,only_ext=None
        ,filename_input_regex=None
        ,key_output_format=None
        ,enable_progress_report=False
        ,write_done=False
        ,**_):
    
    if key_output_format is not None:
        if filename_input_regex is None:
            raise Exception("-ifnr and -okf options work in tandem")
    
    if filename_input_regex is not None:
        logging.info("Compiling input filename regex...")
        try:
            ireg=re.compile(filename_input_regex.strip("'"))
            ofmt=key_output_format.strip("'")
        except:
            raise Exception("Can't compile input filename regex pattern")
        
        if key_output_format is None:
            raise Exception("Input filename regex specified but no output S3 key format specified")
    else:
        ireg=None
        ofmt=None
    
    
    #if args.enable_debug:
    #    logger=logging.getLogger()
    #    logger.setLevel(logging.DEBUG)
    
    bucket_name=bucket_name.strip()
    path_source=path_source.strip()
    
    try:    prefix=prefix.strip()
    except: prefix=None
    
    try:    path_moveto=path_moveto.strip()
    except: path_moveto=None
    
    if path_check is not None:
        code, path_check=resolve_path(path_check)
        if not code.startswith("ok"):
            logging.warning("path_check '%s' might be in error..." % path_check)
    
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
    
    ### wait for 'source' path to be available
    logging.info("Waiting for source path to be accessible... CTRL-c to stop")
    while True:
        if os.path.isdir(p_src):
            break
        sleep(1)
    logging.info("* Source path accessible")

    if path_moveto is not None:
        logging.info("Creating 'moveto' directory if required")
        code, _=mkdir_p(p_dst)
        if not code.startswith("ok"):
            raise Exception("Can't create 'moveto' directory: %s" % p_dst)
        logging.info("* Created moveto directory")
    
    if not enable_simulate:
        try:
            conn = boto.connect_s3()
        except:
            ## not much we can do
            ## but actually no remote calls are made
            ## at this point so it should be highly improbable
            raise Exception("Can't 'connect' to S3")
    
    if not enable_simulate:
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

    ppid=os.getppid()
    logging.info("Process pid: %s" % os.getpid())
    logging.info("Parent pid:  %s" % ppid)
    while True:
        if os.getppid()!=ppid:
            logging.warning("Parent terminated... exiting")
            break
        #################################################

        _code, path_exists=safe_path_exists(path_check)
                        
        if path_check is None or path_exists:
            try: 
                count=0
                gen=gen_walk(p_src, max_files=num_files,only_ext=only_ext)
                
                if gen is not None:                    
                    for src_filename in gen:
                        
                        if write_done:
                            if is_done_file(src_filename):
                                continue
                        
                        if enable_progress_report:
                            logging.info("Processing file: %s" % src_filename)
                        try:          
                            s3key_name=gen_s3_key(ireg, ofmt, p_src, src_filename, prefix)
                        except Exception,e:
                            raise Exception("Error generating S3 key... check your command line parameters... use the 'simulate' facility: %s" % e)
                        
                        if enable_simulate:
                            simulate(src_filename, s3key_name, enable_delete, p_dst)
                        else:
                            k=S3Key(bucket)
                            k.key=s3key_name
                            was_uploaded=process_file(enable_progress_report, bucket_name, prefix, k, src_filename, p_dst, enable_delete, propagate_error)
                            if was_uploaded:
                                count=count+1
                                if write_done:
                                    do_write_done(src_filename)
    
            except Exception, e:
                logging.error("Error processing files...(%s)" % str(e))
        else:
            logging.info()

        if count>0:
            logging.info("Progress> uploaded %s files" % count)

        #####################################################
        logging.debug("...sleeping for %s seconds" % polling_interval)
        sleep(polling_interval)

    
def is_done_file(filename):
    return filename.endswith("done")

    
def do_write_done(src_filename):
    dfile="%s.done" % src_filename
    code, _=touch(dfile)
    if code!="ok":
        raise Exception("Can't write '.done' file: %s" % dfile)

def gen_s3_key(ireg, ofmt, p_src, filename, prefix):
    _, fn=remove_common_prefix(p_src, filename)
    
    ### use the input regex and output format string
    ### to generate S3 key
    if ireg is not None:
        g=ireg.match(fn).groups()
        fn=ofmt % g
    
    s="/%s%s" % (prefix, fn)
    return s.replace("//","")
    
    
    
def simulate(fil, s3key_name, enable_delete, p_dst):
    pprint_kv("File to be uploaded", fil)
    pprint_kv(" Filename used on s3", s3key_name)
    
    if enable_delete:
        _c, f_is_writable=can_write(fil)      
        pprint_kv(" File would be deleted", fil)
        if not f_is_writable:
            pprint_kv(" ! File can't be deleted", fil)
    else:
        if p_dst is not None:
            bname=os.path.basename(fil)
            fdst=os.path.join(p_dst, bname)        
            _c, d_is_writable=can_write(p_dst)
            pprint_kv(" File would be moved to", fdst)
            if not d_is_writable:
                pprint_kv(" ! File can't be moved to", p_dst)


def process_file(enable_progress_report, bucket_name, prefix, k, src_filename, p_dst, enable_delete, propagate_error):
    
    uploaded=False
    ctx={"src": src_filename, "key": k.name, "bucket": bucket_name, "prefix": prefix}
    
    #1) Upload to S3
    try:
        k.set_contents_from_filename(src_filename)
        report(ctx, {"code":"ok", "kind":"upload"})
        
        if enable_progress_report:
            logging.info("progress: uploaded file %s" % src_filename)
            uploaded=True
    except:
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
        else:
            if enable_progress_report:
                logging.info("progress: deleted file %s" % src_filename)

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
        else:
            if enable_progress_report:
                logging.info("progress: moved file %s ==> %s" % (src_filename, dst_filename))

        report(ctx, {"code":code, "kind":"move", "dst":dst_filename})
    
    return uploaded

