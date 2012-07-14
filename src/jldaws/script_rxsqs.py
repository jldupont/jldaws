"""
    Created on 2012-01-19
    @author: jldupont
"""
import logging, sys, os, json
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
        retry_always=None, wait_trigger=None,
        trigger_none_msg=None, trigger_topic=None,
        delete_on_error=False, dont_pass_through=False,
        simulate_error=False, error_msg=None
        ,**_ ):
    
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
    ppid=os.getppid()
    logging.info("Process pid: %s" % os.getpid())
    logging.info("Parent pid: %s" % ppid)
    logging.info("Starting loop...")
    while True:
        if os.getppid()!=ppid:
            logging.warning("Parent terminated... exiting")
            break
        
        
        if wait_trigger or trigger_topic is not None:
            line=sys.stdin.readline().strip()

            if not dont_pass_through:
                stdout(line)

        if trigger_topic is not None:
            if len(line)==0:
                continue
            try:
                jo=json.loads(line)
            except:
                logging.error("Can't JSON decode: %s" % line)
                continue
            
            try:
                topic=jo["topic"]
                #logging.info("Found topic: %s" % topic)
                if topic!=trigger_topic:
                    continue
            except:
                logging.error("Input JSON doesn't have a 'topic' field")
                continue
            
       
        try:
            if simulate_error:
                raise Exception("Network error simulation")
            
            msgs=q.get_messages(num_messages=batch_size)
            error_count=0
        except Exception, e:
            
            if error_msg is not None:
                try:    msg=error_msg % str(e)
                except: msg=error_msg
                stdout(msg)
                continue
                
            if propagate_error:
                stdout('''{"error": "%s"}''' % str(e))
                
            if retry_always:
                continue
        
            error_count=error_count+1
            if error_count<MAX_ERROR_COUNT:
                continue
            
            raise Exception("Exiting because of excessive network or decode error")
        
        ### take care of sending a specified string when
        ### there are no messages in the queue & we are in "trigger mode"
        if msgs is None or len(msgs)==0:
            if wait_trigger or trigger_topic is not None:
                if trigger_none_msg is not None:
                    stdout(trigger_none_msg)
                    continue
            
        ### normal flow
        if msgs is not None:
            for msg in msgs:
                
                try:
                    b=msg.get_body()
                                        
                    if format_any:
                        stdout(b)
                    else:
                        stdout(json.dumps(b))
                        
                    q.delete_message(msg)
                except Exception, e:
                    if delete_on_error:
                        q.delete_message(msg)
                    logging.error("Can't process received msg: %s --> %s" % (str(b), e))
        
        if not wait_trigger and trigger_topic is None:
            logging.debug("...sleeping for %s seconds" % polling_interval)
            sleep(polling_interval)

