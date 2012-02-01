"""
    Created on 2012-01-20
    @author: jldupont
"""
import os, sys, logging
from time import sleep

import boto
from boto.s3.key import Key as S3Key

def run(dst_path=None, topic_name=None, mod_sub=None, mod_pub=None, polling_interval=None):
    
    
    while True:
        
        
        logging.debug("... sleeping for %s seconds" % polling_interval)
        sleep(polling_interval)
