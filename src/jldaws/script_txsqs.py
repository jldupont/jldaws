"""
    Created on 2012-01-19
    @author: jldupont
"""
import logging, json, sys
import boto
from time import sleep

from boto.sqs.jsonmessage import JSONMessage
from boto.sqs.message import RawMessage

from tools_logging import info_dump

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
    format_any=args.format_any
   
    info_dump(args, 20)
    
    # SETUP PRIVATE QUEUE
    try:
        conn = boto.connect_sqs()
        q=conn.create_queue(queue_name)
        if not format_any:
            q.set_message_class(JSONMessage)
        else:
            q.set_message_class(RawMessage)
        
    except Exception,e:
        raise Exception("Creating queue '%s': %s" % (queue_name, str(e)))

    if flush_queue:
        try:    
            q.clear()
            logging.info("queue flushed")
        except: pass

    logging.debug("Starting loop...")
    while True:
        line=sys.stdin.readline()
        
        ## echo on stdout
        stdout(line)
        
        if not format_any:
            try:
                jo=json.loads(line)
            except:
                logging.error("Can't load json object from: %s" % line)
                continue
            
            m=JSONMessage()
            m.set_body(jo)
        else:
            m=RawMessage()
            m.set_body(line)
        
        logging.debug("Writing to SQS queue '%s': %s" % (queue_name, line))
        try:
            q.write(m)
        except:
            ### give us a chance...
            sleep(1)
            try:
                q.write(m)
            except Exception,e:
                logging.error("Can't write to SQS queue - 2nd attempt in a row: %s" % e)
        
