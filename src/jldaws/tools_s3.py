"""
    Created on 2012-01-20
    @author: jldupont
"""
import boto
from boto.exception import S3ResponseError

def keys_to_dict(keys, d={}):
    """
    boto.s3.keys to dictionary
    """
    for key in keys:
        d[key.name]=key
    return d

    
def check_changes(ckeys, keys):
    """
    :param ckeys: current keys data
    :param keys:  keys data just retrieved
    """
    changed=[]
    for key in keys:
        etag=keys[key].etag
        ckey=ckeys.get(key, None)
        cetag=ckey.etag if ckey is not None else None
        
        if etag!=cetag:
            changed.append(key)
    return changed


def bucket_status(bucket_name, prefix):
    
    try:
        conn=boto.connect_s3()
    except:
        raise Exception("Can't connect to S3")
    
    try:
        bucket=conn.get_bucket(bucket_name)
    except S3ResponseError:
        raise Exception("Can't find specified bucket '%s'" % bucket_name)

    try:
        keys=bucket.get_all_keys(prefix=prefix)
    except S3ResponseError:    
        pass
     
    return ("ok", keys)
