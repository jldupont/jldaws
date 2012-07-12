"""
    Created on 2012-01-20
    @author: jldupont
"""
import logging, os

import boto
from boto.s3.key import Key as S3Key

def run(bucket_name=None, bucket_prefix=None, 
        alternate_format=False,
        just_basename=False
        ):
    
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
    
    """
        s3://bucket_name/bucket_prefix/key
        s3://bucket_name/key
        bucket_name /t bucket_prefix/key
    """
    
    if alternate_format:
        _base_format="%s\t" % bucket_name
    else:
        _base_format="s3://%s/" % bucket_name
    
    if bucket_prefix is not None:
        liste=bucket.list(prefix=bucket_prefix)
    else:
        liste=bucket.list()
        
    
    for key in liste:
        if not key.name.endswith("/"):
            if just_basename:
                print os.path.basename(key.name)
            else:
                print "%s%s" % (_base_format, key.name)
