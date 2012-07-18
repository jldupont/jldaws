"""
    Created on 2012-01-20
    @author: jldupont
"""
import logging, os, sys, json

from jldaws.tools_sys import retry
from jldaws.db.skv import SimpleKV

import boto


def run(domains_list=False 
        ,domain_name=None
        ,domain_delete=False
        ,domain_create=False
        ,**_
        ):

    def connect():
        return boto.connect_sdb()
    
    logging.info("Connecting to SDB... CTRL-C to abort")
    db=retry(connect)
    
    if domains_list:
        logging.info("Getting all domains...")
        
        try:
            domains=db.get_all_domains()
            for domain in domains:
                print domain.name
        except Exception,e:
            logging.error("Can't retrieve all domains: %s" % e)
            return 
        
    if domain_delete or domain_create:
        if domain_name is None:
            logging.error("Domain name must be specified")
            return
        
    if domain_delete:
        try:
            logging.info("Deleting domain: %s" % domain_name)
            db.delete_domain(domain_name)
            logging.info("Progress: deleted domain: %s" % domain_name)
        except Exception,e:
            logging.error("Error deleting domain: %s" % e.error_message)
            return
        
    if domain_create:
        try:
            logging.info("Creating domain: %s" % domain_name)
            db.create_domain(domain_name)
            logging.info("Progress: created domain: %s" % domain_name)
        except Exception,e:
            logging.error("Error deleting domain: %s" % e.error_message)
            return
    
        