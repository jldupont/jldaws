"""
    Created on 2012-01-19
    @author: jldupont
"""
import logging
from time import sleep
import boto
from tools_sqs import gen_queue_name
from boto.exception import SQSDecodeError
from boto.sqs.jsonmessage import JSONMessage


def process_msg(args, msg):
    """
    """
    print msg


def run(args):
    """
    1. create private SQS queue
    2. subscribe queue to the specified 'topic'
    """
    try:
        conn = boto.connect_sqs()  
        queue_name=gen_queue_name()
        q=conn.create_queue(queue_name)
        q.set_message_class(JSONMessage) 
    except Exception,e:
        raise Exception("Creating queue '%s': %s" % (queue_name, str(e)))
        
    logging.info("path_script:      %s" % args.path_script[0])
    logging.info("queue_name:       %s" % queue_name)
    logging.info("polling_interval: %s" % args.polling_interval)
    
    while True:
        try:
            msg=q.read()
            if msg is not None:
                process_msg(args, msg)
                q.delete_message(msg)
        except SQSDecodeError:
            t="message decoding error"
            try:
                logging.warning(t+": %s" % msg.get_body())
            except:
                try:
                    logging.warning(t+": %s" % str(msg._body))
                except:
                    logging.warning(t)
            
        sleep(args.polling_interval)
        
    ### should never get here
    print "#e Exiting..."
    

    
    
    
