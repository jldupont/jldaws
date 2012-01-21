"""
    Created on 2012-01-19
    @author: jldupont
"""
import logging, os, json, importlib
from time import sleep
import boto
import json
import sys

from boto.exception import SQSDecodeError
from boto.sqs.jsonmessage import JSONMessage

from tools_sqs import gen_queue_name
from tools_sns import build_topic_arn
from tools_mod import call

def stdout(s):
    sys.stdout.write(s+"\n")

def process_msg(args, queue_name, topic, module_name, msg):
    
    result=call(module_name, "run", queue_name, topic, msg.get_body())
    if result is not None:
        logging.info("result: %s" % str(result))
        
def maybe_json_to_stdout(enable_json, queue_name, topic, msg):
    if enable_json:
        
        try:
            mo=msg.get_body()
            
            o={ "queue_name": queue_name
              ,"topic": topic
              ,"msg": mo
              }
            stdout(json.dumps(o))
        except Exception, e:
            logging.error("can't output json to stdout: %s" % str(e))

def run(args):
    """
    0. check 'module_name'
    1. create private SQS queue
    2. subscribe queue to the specified 'topic'
    """
    enable_json=args.enable_json
    module_name=args.module_name
    enable_call_run=True if module_name.lower()!="none" else False
    batch_size=args.batch_size
   
    logging.info("module_name=      %s" % module_name)    
    logging.info("batch_size=       %s" % batch_size)
    logging.info("polling_interval= %s (seconds)" % args.polling_interval)
    logging.info("json to stdout=   %s" % enable_json)
        
    # SETUP PRIVATE QUEUE
    try:
        conn = boto.connect_sqs()  
        queue_name=gen_queue_name()
        q=conn.create_queue(queue_name)
        q.set_message_class(JSONMessage) 
        
    except Exception,e:
        raise Exception("Creating queue '%s': %s" % (queue_name, str(e)))

    logging.info("queue_name=       %s" % queue_name)
    
    topic=args.topic
    topic_arn=build_topic_arn(conn, topic)
    
    logging.info("topic=            %s" % topic)
    logging.info("topic_arn=        %s" % topic_arn)    
    
    # SUBSCRIBE TO TOPIC
    try:
        snsconn=boto.connect_sns()
        snsconn.subscribe_sqs_queue(topic_arn, q)
        logging.info("Subscribed to topic '%s'" % topic)
    except Exception,e:
        raise Exception("Subscribing to topic '%s': %s" % (topic, str(e)))
    
    logging.debug("Starting loop...")
    while True:
        try:
            msgs=q.get_messages(num_messages=batch_size)
            if msgs is not None:
                for msg in msgs:
                    if enable_call_run:
                        process_msg(args, queue_name, topic, module_name, msg)
                    maybe_json_to_stdout(enable_json, queue_name, topic, msg)
                    q.delete_message(msg)
                
        except SQSDecodeError:
            logging.warning("Message decoding error")
            
        logging.debug("...sleeping for %s seconds" % args.polling_interval)
        sleep(args.polling_interval)
        
