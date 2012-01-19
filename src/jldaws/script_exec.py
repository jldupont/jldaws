"""
    Created on 2012-01-19
    @author: jldupont
"""
import logging, os, json, importlib
from time import sleep
import boto

from boto.exception import SQSDecodeError
from boto.sqs.jsonmessage import JSONMessage

from tools_sqs import gen_queue_name
from tools_sns import build_topic_arn

def process_msg(args, queue_name, topic, mod, msg):
    
    result=mod.run(queue_name, topic, msg.get_body())
    if result is not None:
        logging.info("result: %s" % str(result))
        

def run(args):
    """
    0. check 'module_name'
    1. create private SQS queue
    2. subscribe queue to the specified 'topic'
    """
    
    # CHECK CALLABLE
    module_name=args.module_name[0]
    try:
        mod=importlib.import_module(module_name)
    except:
        raise Exception("Can't import module '%s'" % module_name)
    
    logging.info("module_name=      %s" % module_name)
    
    try:
        run=getattr(mod, "run")
    except:
        raise Exception("Module '%s' doesn't have a 'run' function" % mod)
    
    if not callable(run):
        raise Exception("Can't call 'run' function of callable '%S'" % module_name)

    logging.info("polling_interval= %s (seconds)" % args.polling_interval)
        
    # SETUP PRIVATE QUEUE
    try:
        conn = boto.connect_sqs()  
        queue_name=gen_queue_name()
        q=conn.create_queue(queue_name)
        q.set_message_class(JSONMessage) 
        
    except Exception,e:
        raise Exception("Creating queue '%s': %s" % (queue_name, str(e)))

    logging.info("queue_name=       %s" % queue_name)
    
    topic=args.topic[0]
    topic_arn=build_topic_arn(conn, topic)
    
    logging.info("topic=            %s" % topic)
    logging.info("topic_arn=        %s" % topic_arn)    
    
    # SUBSCRIBE TO TOPIC
    try:
        snsconn=boto.connect_sns()
        snsconn.subscribe_sqs_queue(topic_arn, q)
    except Exception,e:
        raise Exception("Subscribing to topic '%s': %s" % (topic, str(e)))
    
    while True:
        try:
            msg=q.read()
            if msg is not None:
                process_msg(args, queue_name, topic, mod, msg)
                q.delete_message(msg)
                
        except SQSDecodeError:
            logging.warning("Message decoding error")
            
        sleep(args.polling_interval)
        
