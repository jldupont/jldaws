"""
    Created on 2012-01-20
    @author: jldupont
"""
import os, logging

from tools_os      import resolve_path

import boto
from boto.s3.key import Key as S3Key


def run(enable_simulate=False, bucket_name=None, 
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
    

    if enable_simulate:
        logging.info("Begin simulation...")

