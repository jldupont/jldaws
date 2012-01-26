"""
    Created on 2012-01-20
    @author: jldupont
"""
import json
import boto
from boto.exception import S3ResponseError

def keys_to_dict(keys):
    """
    boto.s3.keys to dictionary
    """
    d={}
    for key in keys:
        d[key.name]=key
    return d


def compute_changes(ckeys, nkeys):
    """
    Compute changes in keys
    """
    additions=[]
    substractions=[]
    changes=[]
    
    nset=set(nkeys.keys())
    cset=set(ckeys.keys())
    
    #print "cset: %s" % cset
    #print "nset: %s" % nset
    
    for k in nset:
        if k not in cset:
            additions.append(k)
            
    for k in cset:
        if k not in nset:
            substractions.append(k)
            
    common=cset.intersection(nset)
    for k in common:
        cetag=ckeys[k].etag
        netag=nkeys[k].etag
        
        if cetag != netag:
            changes.append(k)
            
    return (changes, additions, substractions)
            
    
def check_changes(ckeys, nkeys):
    """
    :param ckeys: current keys data
    :param keys:  keys data just retrieved
    """
    all_keys=set(nkeys.keys())
    all_keys.update(ckeys.keys())
    print all_keys
    changed=[]
    for k in all_keys:
        nkey=nkeys.get(k, None)
        ckey=ckeys.get(k, None)
        
        netag=nkey.etag if nkey is not None else None
        cetag=ckey.etag if ckey is not None else None
        
        print "etags:", cetag, netag
        
        if netag!=cetag:
            changed.append(k)
    return changed

def json_string(o):
    try:
        return json.dumps(o)
    except:
        return ""


KEY_FIELDS=["content_type", "content_encoding", "etag", "last_modified", "storage_class"]
def key_object_to_dict(key):
    """
    Expects a boto.s3.key.Key object type
    """
    d={}
    for k in KEY_FIELDS:
        d[k]=str(key.__dict__[k])
        
    return { key.name: d}
    

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
