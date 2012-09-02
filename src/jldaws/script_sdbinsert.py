"""
    Created on 2012-01-20
    @author: jldupont
"""
import logging, os, sys, json
from time import sleep

from jldaws.tools_os import rm
from jldaws.tools_sys import retry
from jldaws.db.skv import SimpleKV

def run(domain_name=None 
        ,category_name=None
        ,default_value=None
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
            
        if len(line)==0 or line=="":   ## safe than sorry
            logging.debug("Empty input line...")
            continue
        
        try:
            ino=json.loads(line)
            ino["category"]=ino.get("category", category_name)
            ino["value"]=ino.get("value", default_value)
            if ino.get("key", None) is None:
                logging.warning("Missing 'key' field on line: %s" % line)
                continue
            logging.debug("Input JSON: %s" % ino)
        except:
            ino={}
            try:
                bits=line.split("\t")
                if len(bits)>3 or len(bits)<1:
                    logging.warning("Invalid text input: %s" % line)
                    continue
                if len(bits)==3:
                    ino["category"]=bits[0]
                    ino["key"]=bits[1]
                    ino["value"]=bits[2]
                if len(bits)==2:
                    ino["category"]=category_name
                    ino["key"]=bits[0]
                    ino["value"]=bits[1]
                if len(bits)==1:
                    ino["category"]=category_name
                    ino["key"]=bits[0]
                    ino["value"]=default_value
                logging.debug("Input TEXT: %s" % ino)
            except:
                logging.warning("Error processing input: %s" % line)
                continue
        
        success=False
        try:
            db.insert(ino["key"], ino["value"], category=ino["category"])
            logging.info("Progress: inserted 1 record")
            success=True
        except:
            try:
                ### don't give up too easily
                sleep(1)
                db.insert(ino["key"], ino["value"], category=ino["category"])
                logging.info("Progress: inserted 1 record (after 2nd try)")
                success=True
            except Exception,e:
                logging.debug(e)
                logging.warning("Can't insert in SDB...")
        
        if success:
            trackerfile=ino.get("trackerfile", None)
            if trackerfile is not None:
                code, msg=rm(trackerfile)
                if code!="ok":
                    logging.warn("Can't delete tracker file %s: %s" % (trackerfile, msg))
            
                    
            
        
        