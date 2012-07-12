"""
    Created on 2012-01-20
    @author: jldupont
"""
import logging, os, sys, json

import boto
from boto.s3.key import Key as S3Key

from jldaws.tools_sys import retry

def run(bucket_name=None, bucket_prefix=None, 
        alternate_format=False,
        just_basename=False,
        trigger_topic=None,
        **_
        ):

    try:
        conn = boto.connect_s3()
    except:
        ## not much we can do
        ## but actually no remote calls are made
        ## at this point so it should be highly improbable
        raise Exception("Can't 'connect' to S3")

    logging.info("Creating bucket...")
    
    def bucket_create():
        return conn.create_bucket(bucket_name)

    try:
        bucket=retry(bucket_create)
        logging.info("Got bucket '%s'" % bucket_name)
    except:
        raise Exception("Can't get bucket '%s'" % bucket_name)
    
    
    # MAIN LOOP
    ###########
    ppid=os.getppid()
    logging.info("Process pid: %s" % os.getpid())
    logging.info("Parent pid: %s" % ppid)
    logging.info("Starting loop...")
    while True:
        if os.getppid()!=ppid:
            logging.warning("Parent terminated... exiting")
            break

        line=sys.stdin.readline().strip()

        if trigger_topic is not None:
            if len(line)==0:
                continue
            try:
                jo=json.loads(line)
                logging.debug("Loaded JSON...")
            except:
                logging.error("Can't load JSON from line: %s" % line)
                continue
        
            try:
                topic=jo["topic"]
                logging.debug("Extracted topic: %s" % topic)
            except:
                logging.error("JSON object doesn't have 'topic' field")
                continue
        
            if topic!=trigger_topic:
                logging.debug("Topic != trigger_topic: %s" % topic)
                continue
            
        logging.debug("Triggered...")
        
        
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
