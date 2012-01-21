"""
    Created on 2012-01-19
    @author: jldupont
"""
import logging, json, sys
import boto
from time import sleep

from boto.sqs.jsonmessage import JSONMessage


def stdout(s):
    sys.stdout.write(s+"\n")

def run(args):
    
    queue_name=args.queue_name
    flush_queue=args.flush_queue
   
    logging.info("queue_name=      %s" % queue_name)    
    logging.info("flush_queue=     %s" % flush_queue)
        
    # SETUP PRIVATE QUEUE
    try:
        conn = boto.connect_sqs()
        q=conn.create_queue(queue_name)
        q.set_message_class(JSONMessage) 
        
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
        
        try:
            jo=json.loads(line)
        except:
            logging.error("Can't load json object from: %s" % line)
            continue
        
        m=JSONMessage()
        m.set_body(jo)
        
        logging.debug("Writing to SQS queue '%s': %s" % (queue_name, line))
        try:
            q.write(m)
        except:
            ### give us a chance...
            sleep(1)
            try:
                q.write(m)
            except:
                logging.error("Can't write to SQS queue - 2nd attempt in a row")
        
