"""
    Created on 2012-01-20
    @author: jldupont
"""
import logging
from time import sleep
import uuid

from tools_sys import retry
from tools_mod import prepare_callable
from tools_sqs import gen_queue_name
from tools_sns import build_topic_arn
from tools_sys import coroutine
import boto
from boto.sqs.jsonmessage import JSONMessage
from boto.exception import SQSDecodeError

def run(dst_path=None, topic_name=None, queue_name=None, mod_sub=None, mod_pub=None, polling_interval=None, force=False, node_id=None):

    if node_id is None:
        node_id=str(uuid.uuid1())
        
    logging.info("Node id: %s" % node_id)

    proc=protocol_processor(node_id, dst_path)
    
    if force:
        run_force(node_id, proc, polling_interval, dst_path)
    else:
        if mod_pub is None or mod_sub is None:
            run_aws(node_id, proc, polling_interval, queue_name, topic_name, dst_path)
        else:
            run_mod(node_id, proc, polling_interval, topic_name, dst_path, mod_sub, mod_pub)


        
def run_aws(node_id, proc, polling_interval, queue_name, topic_name, dst_path):

    if topic_name is None:
        raise Exception("Need a topic_name")
    
    if queue_name is None:
        queue_name=gen_queue_name()

    def setup_private_queue():
        conn = boto.connect_sqs()
        q=conn.create_queue(queue_name)
        q.set_message_class(JSONMessage)
        return (conn, q) 

    # SETUP PRIVATE QUEUE
    logging.info("Creating queue '%s': %s" % queue_name)
    conn, q=retry(setup_private_queue, logmsg="Having trouble creating queue...")
    
    topic_arn=build_topic_arn(conn, topic_name)
    
    logging.info("topic_arn: %s" % topic_arn)    
    
    # SUBSCRIBE TO TOPIC
    def sub_topic():
        snsconn=boto.connect_sns()
        snsconn.subscribe_sqs_queue(topic_arn, q)

    retry(sub_topic, logmsg="Having trouble subscribing queue to topic...")
    
    logging.info("Subscribed to topic '%s'" % topic_name)
        
    logging.info("Starting loop...")
    while True:
        
        try:
            while True:
                msg=q.read()
                if msg is not None:
                    proc.send(msg)
                    q.delete_message(msg)
                else:
                    break
                
        except SQSDecodeError:
            logging.warning("Message decoding error")
    
        
        logging.debug("... sleeping for %s seconds" % polling_interval)
        sleep(polling_interval)


def run_force(node_id, proc, polling_interval, dst_path):
    pass


def run_mod(node_id, proc, polling_interval, topic_name, dst_path, mod_sub, mod_pub):
    fnc_sub=prepare_callable(mod_sub, "subscribe_message")
    fnc_pub=prepare_callable(mod_sub, "publish_message")
    
    logging.info("Starting loop...")
    while True:
       
        while True:
            try:
                code, maybe_msg=fnc_sub(topic_name)
                if not code.startswith("ok"):
                    logging.error("Error retrieving message: %s" % maybe_msg)
                    break
                else:
                    if maybe_msg is not None:
                        proc.send(maybe_msg)
            except:
                pass

        fnc_pub(topic_name, {})
        logging.debug("... sleeping for %s seconds" % polling_interval)
        sleep(polling_interval)
        

@coroutine
def protocol_processor(node_id, dst_path):
    
    while True:
        msg=(yield)
        

