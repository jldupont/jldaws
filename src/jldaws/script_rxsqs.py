"""
    Created on 2012-01-19
    @author: jldupont
"""
import logging, sys
import boto
from time import sleep

from boto.sqs.jsonmessage import JSONMessage
from boto.sqs.message     import RawMessage

from tools_sys import retry

MAX_ERROR_COUNT=25

def stdout(s):
    try:
        if not s.endswith("\n"):
            s=s+"\n"
    except:
        pass
    sys.stdout.write(s)
    sys.stdout.flush()

def run(queue_name=None, flush_queue=None,
        batch_size=None, polling_interval=None,
        format_any=None, propagate_error=None,
        retry_always=None, wait_trigger=None ):
    
    ## we need a minimum of second between polls
    polling_interval=max(1, polling_interval)
   
    # SETUP PRIVATE QUEUE
    #####################
    def setup_private_queue():
        """ closure for setting up private queue """
        conn = boto.connect_sqs()
        q=conn.create_queue(queue_name)
        if format_any:
            q.set_message_class(RawMessage)
        else:
            q.set_message_class(JSONMessage)
        return q 
        
    try:
        q=retry(setup_private_queue, always=retry_always)
        logging.info("Queue successfully created") 
    except Exception,e:
        raise Exception("Creating queue '%s': %s" % (queue_name, str(e)))

    # FLUSH QUEUE
    #############
    if flush_queue:
        try:    
            q.clear()
            logging.info("Queue flushed")
        except: pass

    error_count=0

    # MAIN LOOP
    ###########
    logging.info("Starting loop...")
    while True:
        
        if wait_trigger:
            _=sys.stdin.readline()
        
        try:
            msgs=q.get_messages(num_messages=batch_size)
            error_count=0
        except Exception, e:
            if propagate_error:
                stdout('''{"error": "%s"}''' % str(e))
            if retry_always:
                continue
        
            error_count=error_count+1
            if error_count<MAX_ERROR_COUNT:
                continue
            
            raise Exception("Exiting because of excessive decode error")
            
        ### normal flow
        if msgs is not None:
            for msg in msgs:
                
                try:    b=msg._body
                except: b=msg
                
                try:
                    stdout(str(b))
                    q.delete_message(msg)
                except Exception, e:
                    logging.error("Can't process received msg: %s --> %s" % (str(b), e))
        
        logging.debug("...sleeping for %s seconds" % polling_interval)
        sleep(polling_interval)

