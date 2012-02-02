"""
    Created on 2012-01-20
    @author: jldupont
"""
import logging, os, json, signal
from time import sleep
import uuid

from tools_os import can_write, rm
from tools_sys import retry, SignalTerminate
from tools_mod import prepare_callable
from tools_sqs import gen_queue_name
from tools_sns import build_topic_arn
from tools_leader import protocol_processor

import boto
from boto.sqs.message import RawMessage
from boto.exception import SQSDecodeError

sigtermReceived=False

def handlerSigTerm(*p, **k):
    global sigtermReceived
    sigtermReceived=True

def run(dst_path=None, topic_name=None, queue_name=None, mod_sub=None, mod_pub=None, 
        polling_interval=None, force=False, node_id=None, force_delete=False,
        proto_n=None, proto_m=None):

    signal.signal(signal.SIGTERM, handlerSigTerm)

    ### STARTUP CHECKS
    ##################
    
    if proto_n > proto_m:
        raise Exception("Parameter 'n' must be smaller than 'm'")
    
    if os.path.isdir(dst_path):
        raise Exception("'dst_path' must be a filename, not a directory")

    if force_delete:
        logging.info("Attempting to delete '%s'" % dst_path)
        rm(dst_path)

    if os.path.isfile(dst_path):
        raise Exception("'dst_path' must not exists at startup: use -fd to delete")
        
    dir_path=os.path.dirname(dst_path)
    code, _msg=can_write(dir_path)
    if not code.startswith("ok"):
        raise Exception("directory '%s' is not writable" % dir_path)
    
    ### SETUP
    ###########################
    if node_id is None:
        node_id=str(uuid.uuid1())
        
    logging.info("Node id: %s" % node_id)

    proc=protocol_processor(node_id, proto_n, proto_m)
    
    
    ### START MAIN LOOP
    ######################################
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
        q.set_message_class(RawMessage)
        return (conn, q) 

    # SETUP PRIVATE QUEUE
    logging.info("Creating queue '%s'" % queue_name)
    conn, q=retry(setup_private_queue, logmsg="Having trouble creating queue...")
    
    topic_arn=build_topic_arn(conn, topic_name)
    
    logging.info("topic_arn: %s" % topic_arn)    
    
    ### create topic
    def create_topic():
        """
        {'CreateTopicResponse': 
            {'ResponseMetadata': 
                {'RequestId': '5e2c6700-4dd0-11e1-b421-41716ce69b95'}, 
            'CreateTopicResult': {'TopicArn': 'arn:aws:sns:us-east-1:674707187858:election'}}}
        """
        snsconn=boto.connect_sns()
        snsconn.create_topic(topic_name)
        
    retry(create_topic, logmsg="Having trouble creating topic...")
    
    # SUBSCRIBE TO TOPIC
    def sub_topic():
        snsconn=boto.connect_sns()
        snsconn.subscribe_sqs_queue(topic_arn, q)
        return snsconn

    snsconn=retry(sub_topic, logmsg="Having trouble subscribing queue to topic...")
    
    logging.info("Subscribed to topic '%s'" % topic_name)
        
    current_state="NL"
    
    MSGS={"NL": "Leadership lost",
          "L":  "Leadership gained",
          "ML": "Leadership momentum"
          }
    
    poll_count=0
    logging.info("Starting loop...")
    
    while True:
        
        global sigtermReceived
        if sigtermReceived:
            raise SignalTerminate()
            
        try:
            ### BATCH PROCESS - required!!!
            while True:
                rawmsg=q.read()
                if rawmsg is not None:
                    jsonmsg=json.loads(rawmsg.get_body())
                    
                    ## SNS encapsulates the original message...
                    nodeid=str(jsonmsg["Message"])
                    
                    transition, current_state=proc.send((poll_count, nodeid))
                    
                    q.delete_message(rawmsg)
                    if transition:
                        logging.info(MSGS[current_state])
                else:
                    break
                
        except SQSDecodeError:
            logging.warning("Message decoding error")
            
        except Exception,e:
            logging.error(str(e))
            continue
    
        msg=str(node_id)
    
        logging.debug("Publishing our 'node id': %s" % node_id)
        try:
            snsconn.publish(topic_arn, msg)
        except:
            try:
                snsconn.publish(topic_arn, msg)
            except:
                logging.warning("Can't publish to topic '%s'" % topic_name)
        
        logging.debug("... sleeping for %s seconds" % polling_interval)
        sleep(polling_interval)
        poll_count=poll_count+1
        

def run_force(node_id, proc, polling_interval, dst_path):
    pass


def run_mod(node_id, proc, polling_interval, topic_name, dst_path, mod_sub, mod_pub):
    fnc_sub=prepare_callable(mod_sub, "subscribe_message")
    fnc_pub=prepare_callable(mod_sub, "publish_message")
    
    poll_count=1
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
                        current_state=proc.send((poll_count, maybe_msg))
            except:
                pass

        try:
            fnc_pub(topic_name, {})
        except Exception, e:
            logging.error("Publish function: %s" % str(e))
        
        logging.debug("... sleeping for %s seconds" % polling_interval)
        sleep(polling_interval)
        poll_count=poll_count+1
        

