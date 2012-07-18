"""
    Created on 2012-01-20
    @author: jldupont
"""
import logging, os, sys, json

from jldaws.tools_sys import retry
from jldaws.db.skv import SimpleKV

def run(domain_name=None, 
        category_name=None,
        trigger_topic=None 
        ,**_
        ):

    db=SimpleKV()
    def connect():
        db.connect()
    
    logging.info("Connecting to SDB... CTRL-C to abort")
    retry(connect)
    
    def create():
        db.set_domain(domain_name)
        
    logging.info("Creating domain: %s... CTRL-C to abort" % domain_name)
    retry(create)
    
    # MAIN LOOP
    ###########
    ppid=os.getppid()
    logging.info("Process pid: %s" % os.getpid())
    logging.info("Parent pid:  %s" % ppid)
    logging.info("Starting loop...")
    while True:
        if os.getppid()!=ppid:
            logging.warning("Parent terminated... exiting")
            break

        try:
            line=sys.stdin.readline().strip()
        except:
            raise Exception("broken stdin...")
            
        
        if trigger_topic is not None:
            if len(line)==0 or line=="":   ## safe than sorry
                continue
            
            try:
                jo=json.loads(line)
            except:
                logging.warning("Can't decode JSON from: %s" % line)
                continue
            
            try:
                topic=jo["topic"]
            except:
                logging.warning("No 'topic' field found: %s" % line)
                continue
            
            if topic!=trigger_topic:
                logging.debug("No specified topic: %s" % topic)
                continue
            
        entries=[]
        next_token=None
        while True:
            try:
                logging.debug("Getting batch...")
                batch, next_token=db.get_by_category(category=category_name, next_token=next_token)
            except TypeError:
                ### no entries
                ### boto isn't too friendly here...
                logging.debug("No entries...")
                break
            except Exception, e:
                logging.warning(e.error_message)
                break
            
            if batch is None:
                break            
            entries.append(batch)
            
            if next_token is None:
                break
            
        for entry in entries:
            print entry
                
            
            
        
