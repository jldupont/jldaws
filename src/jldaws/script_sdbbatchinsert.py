"""
    Created on 2012-01-20
    @author: jldupont
"""
import logging, os, json
from time import sleep

from pyfnc.fnc_tools import coroutine

from jldaws.tools_os import mkdir_p, resolve_path, get_root_files, file_contents, rm
from jldaws.tools_sys import retry
from jldaws.db.skv import SimpleKV

def run(domain_name=None
        ,src_path=None
        ,polling=None 
        ,batch_size=None
        ,category_name=None
        ,default_value=None
        ,src_ext=None
        ,reset_count=None
        ,**_
        ):

    code, src_path=resolve_path(src_path)
    if not code.startswith("ok"):
        raise Exception("Can't resolve source path...")

    logging.info("Creating source path if required...")
    mkdir_p(src_path)

    ######################
    
    db=SimpleKV()
    def connect():
        db.connect()
    
    logging.info("Connecting to SDB... CTRL-C to abort")
    retry(connect)
    
    def create():
        db.set_domain(domain_name)
        
    logging.info("Creating domain: %s... CTRL-C to abort" % domain_name)
    retry(create)
    
    #############
    stage1=process1(db, batch_size, category_name, default_value)
    pollcount=0
    
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

        reset=(pollcount==reset_count)
            
        code, files=get_root_files(src_path, only_ext=src_ext)
        if code.startswith("ok"):
            logging.debug("Got files: %s" % files)
            stage1.send((reset, files))
            
        logging.debug("Sleeping...")
        sleep(polling)
        pollcount=pollcount+1
        

@coroutine
def process1(db, batch_size, category_name, default_value):

    stage2=process2(db, category_name, default_value)
    recent=[]
    
    while True:
        reset, files=(yield)
        
        #qlogging.debug("stage1: received: %s %s" % (reset, files))
        
        if reset:
            recent=[]

        def filter_recent(_file):
            return _file not in recent

        files=filter(filter_recent, files)
            
        files=files[0:batch_size]
        if len(files)!=0:            
            logging.info("Progress> processing %s files" % len(files))
        
            for _file in files:
                recent.append(_file)
                
                code, maybe_contents=file_contents(_file)
                if code.startswith("ok"):
                    stage2.send((_file, maybe_contents))
                else:
                    logging.warning("Can't read contents of: %s" % _file)
            

@coroutine
def process2(db, category_name, default_value):
    
    stage3=process3()
    
    while True:
        _file, contents=(yield)
        logging.info("Progress> processing file: %s" % _file)
    
        inserted=False    
        try:
            ino=json.loads(contents)
            ino["category"]=ino.get("category", category_name)
            ino["value"]=ino.get("value", default_value)
            if ino.get("key", None) is None:
                logging.warning("Missing 'key' field in file: %s" % _file)
            else:
                logging.debug("Input JSON: %s" % ino)
                inserted=do_insert(db, ino["category"], ino["key"], ino["value"])
        except:
            logging.warning("Error processing file: %s" % _file)

        if inserted:
            stage3.send(_file)
        
    
                
def do_insert(db, category, key, value):
    logging.debug("Inserting [%s:%s:%s]" % (category, key, value))
    try:
        db.insert(key, value, category=category)
        logging.info("Progress: inserted 1 record")
        return True
    except:
        try:
            ### don't give up too easily
            sleep(1)
            db.insert(key, value, category=category)
            logging.info("Progress: inserted 1 record (after 2nd try)")
            return True
        except Exception,e:
            logging.debug(e)
            logging.warning("Can't insert in SDB...")
            
    return False
          
@coroutine
def process3():
    """
    Delete source file once processed
    """
    while True:
        
        _file_to_delete=(yield)
        rm(_file_to_delete)
        

    
