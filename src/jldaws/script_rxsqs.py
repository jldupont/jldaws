"""
    Created on 2012-01-19
    @author: jldupont
"""
import logging, sys
import boto
from time import sleep

from boto.sqs.jsonmessage import JSONMessage
from boto.sqs.message     import RawMessage
from boto.exception       import SQSError

from tools_logging import info_dump

MAX_ERROR_COUNT=25

def stdout(s):
    try:
        if not s.endswith("\n"):
            s=s+"\n"
    except:
        pass
    sys.stdout.write(s)

def run(args):
    
    queue_name=args.queue_name
    flush_queue=args.flush_queue
    batch_size=args.batch_size
    polling_interval=args.polling_interval
    format_any=args.format_any
    propagate_error=args.propagate_error
   
    info_dump(args, 20)
   
    # SETUP PRIVATE QUEUE
    try:
        conn = boto.connect_sqs()
        q=conn.create_queue(queue_name)
        if format_any:
            q.set_message_class(RawMessage)
        else:
            q.set_message_class(JSONMessage) 
        
    except Exception,e:
        raise Exception("Creating queue '%s': %s" % (queue_name, str(e)))

    if flush_queue:
        try:    
            q.clear()
            logging.info("queue flushed")
        except: pass

    error_count=0

    logging.debug("Starting loop...")
    while True:
        
        try:
            msgs=q.get_messages(num_messages=batch_size)
        except SQSError, e:
            if propagate_error:
                stdout('''{"error": "%s"}''' % str(e))
            continue
        except Exception:
            error_count=error_count+1
            if error_count<MAX_ERROR_COUNT:
                logging.error("Can't decode received message")
                continue
            raise Exception("Exiting because of excessive decode error")
            
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
