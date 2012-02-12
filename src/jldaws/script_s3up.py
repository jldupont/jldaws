"""
    Created on 2012-01-20
    @author: jldupont
"""
import os, logging

from tools_os import resolve_path, split_path_version, file_contents
from tools_s3 import get_all_keys

import boto
from boto.s3.key import Key as S3Key

def filter_keys(root, keys):
    """
    Just keys beginning with 'root'
    """
    ff=lambda p: p.startswith(root)
    return (keys, filter(ff, keys))
        
def gen_key(prefix, name):
    if prefix is not None:
        return "%s/%s" % (prefix,name)
    return name
    

def run(enable_simulate=False, 
        bucket_name=None, bucket_prefix=None, 
        path_source=None, path_dest=None,
        delete_old=False):
    
    code, path_source=resolve_path(path_source)
    if not code.startswith("ok"):
        logging.warning("Source file '%s' can't be accessed..." % path_source)
    
    
    try:
        conn = boto.connect_s3()
    except:
        ## not much we can do
        ## but actually no remote calls are made
        ## at this point so it should be highly improbable
        raise Exception("Can't 'connect' to S3")
    
    try:
        bucket=conn.create_bucket(bucket_name)
        logging.info("Got bucket '%s'" % bucket_name)        
    except:
        raise Exception("Can't get bucket '%s'" % bucket_name)
    

    base_name=os.path.basename(path_source)
    logging.info("Basename of file to upload: %s" % base_name)

    
    root_name,version,_ext=split_path_version(base_name)
    if version is not None and len(version)>0:
        logging.info("Basename of file: %s" % version)
        logging.info("Version of file:  %s" % version)
    else: 
        version=None
        
    if root_name is None:
        root_name=base_name

    if path_dest is None:
        logging.info("Will be using '%s' as filename in bucket" % base_name)
        path_dest=base_name

    key_names=None
    to_delete=None
    if delete_old:
        logging.info("Getting bucket keys")
        code, bkeys=get_all_keys(bucket, bucket_prefix)
        if not code.startswith("ok"):
            raise Exception("Can't get bucket keys...")
        
        logging.info("Got %s key(s) to filter for 'old' files" % len(bkeys))
        
        _key_names, to_delete=filter_keys(root_name, bkeys)
        
        logging.info("Older files found: %s" % to_delete)
            
    if enable_simulate:
        logging.info("! Begin simulation...")

    code, contents=file_contents(path_source)
    if not code.startswith("ok"):
        raise Exception("Can't read file '%s'" % path_source)
    
    logging.info("Got source file contents")
    
    try:
        upload_key=S3Key(bucket)
        upload_key_name=gen_key(bucket_prefix, path_dest)
        upload_key.key=upload_key_name
            
        logging.info("Prepared S3 key: %s" % upload_key.key)
    except Exception,e:
        raise Exception("S3 key generation: %s" % str(e))
    
    if enable_simulate:
        logging.info("! Would have uploaded file to S3")
    else:
        try:
            upload_key.set_contents_from_string(contents)
            logging.info("Success uploading!")
        except:
            raise Exception("Error uploading file to S3")
        
    if delete_old:
        if enable_simulate:
            logging.info("! Would have deleted the 'old' files: %s" % to_delete)
        else:
            logging.info("Deleting 'old' files:")
            for name in to_delete:
                try:
                    key=S3Key(bucket)
                    key_name=gen_key(bucket_prefix, name)
                    
                    if upload_key_name==key_name:
                        logging.info("Skipping deleting just uploaded file...")
                        continue
                    
                    #### DELETING
                    key.key=key_name
                    bucket.delete_key(key)
                    logging.info("Deleted '%s'" % key.key)
                    
                except Exception,e:
                    logging.warning("Can't delete '%s': %s" % (key.key, str(e)))
    
    logging.info("done")
    
     
    
    