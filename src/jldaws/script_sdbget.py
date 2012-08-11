"""
    Created on 2012-01-20
    @author: jldupont
"""
import logging, os, sys, json
from time import sleep

from jldaws.tools_date import iso_compare_string
from jldaws.tools_os import mkdir_p, resolve_path, atomic_write
from jldaws.tools_sys import retry
from jldaws.db.skv import SimpleKV

from pyfnc.fnc_tools import coroutine

def run(domain_name=None, 
        category_name=None,
        trigger_topic=None 
        ,just_key=False
        ,just_key_value=False
        ,disable_pass_through=False
        ,path_tmp=None
        ,path_dst=None
        ,keep_most_recent=None
        ,ext=None
        ,polling=None
        ,**_
        ):

    mode=path_dst and 'file' or 'stdin' 
    logging.info("Mode is: %s" % mode)

    if path_tmp is not None:
        code, path_tmp=resolve_path(path_tmp)
        if not code.startswith("ok"):
            raise Exception("Can't resolve path: %s" % path_tmp)
        mkdir_p(path_tmp)

    if path_dst is not None:
        code, path_dst=resolve_path(path_dst)
        if not code.startswith("ok"):
            raise Exception("Can't resolve path: %s" % path_dst)
        mkdir_p(path_dst)
    

    db=SimpleKV()
    def connect():
        db.connect()
    
    logging.info("Connecting to SDB... CTRL-C to abort")
    retry(connect)
    
    def create():
        db.set_domain(domain_name)
        
    logging.info("Creating domain: %s... CTRL-C to abort" % domain_name)
    retry(create)
    
    ###########
    flow_stdin=workflow_stdin(trigger_topic, db, category_name, just_key, just_key_value)
    flow_file=workflow_file(db, category_name, path_dst, path_tmp, keep_most_recent, ext)
    poll_count=0
    
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

        if mode is 'stdin':
            try:
                line=sys.stdin.readline().strip()
                if not disable_pass_through:
                    sys.stdout.write(line+"\n")            
            except:
                raise Exception("broken stdin/stdout...")

            flow_stdin.send(line)
            
        if mode is 'file':
            flow_file.send(poll_count)
            
            logging.debug("Sleeping...")
            sleep(polling)
            poll_count=poll_count+1
            
            
def get_batch(db, category_name):
    
    entries=[]
    next_token=None
    while True:
        try:
            logging.debug("Getting batch...")
            batch, next_token=db.get_by_category(category=category_name, next_token=next_token, last=False)
        except Exception, e:
            logging.warning(e)
            batch=None
            break
        
        if batch is None:
            break     
               
        entries.append(batch)
        
        if next_token is None:
            break
        
    return entries
    
@coroutine            
def workflow_stdin(trigger_topic, db, category_name, just_key, just_key_value):
    """for stdin trigger based"""

    while True:
        line=(yield)
        
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
        
        entries=get_batch(db, category_name)
        
        for resultset in entries:
            for entry in resultset:
                
                if just_key:
                    try:    print entry["key"]
                    except:
                        logging.debug("Entry without a 'key' field...")
                        
                if just_key_value:
                    try:    print "%s\t%s" % (entry["key"], entry["value"])
                    except:
                        logging.debug("Entry without a 'key'/'value' field(s)...")
                 
                if not just_key and not just_key_value:       
                    print entry
        
    
@coroutine            
def workflow_file(db, category_name, path_dst, path_tmp, keep_most_recent, ext):
    """for file based workflow"""
    
    if ext is None:
        ext=category_name
        
    proc=process_entry(path_dst, path_tmp, keep_most_recent, ext)
    
    while True:
        _poll_count=(yield)
        
        entries=get_batch(db, category_name)
        logging.debug("Got batch...")
        
        proc.send(("begin", None))
        for resultset in entries:
            for entry in resultset:
                proc.send(("entry", entry))
        proc.send(("end", None))

    
@coroutine    
def process_entry(path_dst, path_tmp, keep_most_recent, ext):
    
    entries={}
    while True:
        code, maybe_entry=(yield)
        #logging.debug("Code: %s" % code)
        
        if code=="begin":
            logging.debug("Got 'begin'...")
            entries={}
        
        if code=="entry":
            logging.debug("Got 'entry'...")
            key=maybe_entry.get("key", None)
            if key is None:
                logging.warning("Entry doesn't have a 'key' field: %s" % maybe_entry)
            else:                
                liste=entries.get(key, [])
                liste.append(maybe_entry)
                entries[key]=liste
        
        if code=="end":
            logging.debug("Got 'end'...")
            entries=filter_entries(keep_most_recent, entries)
            write_entries(path_dst, path_tmp, ext, entries)

            
def filter_entries(keep_most_recent, entries):
    """
    For each key, keep only the latest
    (based on the 'creation_date' field)
    """
    logging.debug("Filtering entries...")
    
    if not keep_most_recent:
        return entries
    
    new_entries={}
    for key in entries:
        entry_to_keep=None
        
        for entry in entries[key]:
            if entry_to_keep is None:
                entry_to_keep=entry
                continue
            try:    
                code, delta=iso_compare_string(entry_to_keep['creation_date'], entry['creation_date'])
                if code is "ok":
                    if delta is "older":
                        entry_to_keep=entry
            except Exception,e:
                print e
        new_entries[key]=[entry_to_keep,]
            
    return new_entries


def write_entries(path_dst, path_tmp, ext, entries):
    
    logging.debug("Writing entries...")
    entries_written=0
    for entry_key in entries:

        filename=os.path.join(path_dst, "%s.%s" % (entry_key, ext))
        records=entries[entry_key]
        ## can't use .join because 'record' is really a boto.sdb.item.Item object
        contents=""
        for record in records:
            try:    contents+=json.dumps(record)
            except:
                logging.warning("Problem with JSON conversion of: %s" % str(record))
                continue

        code, msg=atomic_write(filename, contents, tmppath=path_tmp)
        if not code.startswith("ok"):
            logging.warning("Can't write to '%s': %s" % (filename, msg))
        else:
            entries_written=entries_written+1
        
    if entries_written>0:
        logging.info("Progress> wrote '%s' entries" % entries_written)
        #print contents


