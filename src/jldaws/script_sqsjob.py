"""
    Created on 2012-01-19
    @author: jldupont
"""
from   functools import partial
from   time import sleep
import logging, sys, os, json
import boto

from boto.sqs.jsonmessage import JSONMessage
from boto.sqs.message     import RawMessage

from tools_sys import retry

def stdout(s, json_mode=False):
    if json_mode:
        s=json.dumps(s)
    try:
        if not s.endswith("\n"):
            s=s+"\n"
    except:
        pass
    sys.stdout.write(s)

def run(queue_name=None, 
        queue_name_output=None,
        write_delay=None,
        flush_queue=None,
        batch_size=None,
        format_any=None,
        trigger_none_msg=None, trigger_topic=None,
        delete_on_error=False, dont_pass_through=False,
        simulate_error=False, error_msg=None,
        log_network_error=False
        ,**_):
    
    if write_delay>900 or write_delay<1:
        raise Exception("Invalid value for 'delay_seconds': must be between 1 and 900")
    
    # SETUP PRIVATE QUEUE
    #####################
    def setup_queue(qn):
        """ closure for setting up private queue """
        conn = boto.connect_sqs()
        q=conn.create_queue(qn)
        if format_any:
            q.set_message_class(RawMessage)
        else:
            q.set_message_class(JSONMessage)
        return q 
        
    logging.info("Creating Input Queue... (automatic retry)")
    try:
        qi=retry(partial(setup_queue, queue_name))
        logging.info("Input Queue successfully created") 
    except Exception,e:
        raise Exception("Creating queue '%s': %s" % (queue_name, str(e)))

    logging.info("Creating Output Queue... (automatic retry)")
    try:
        qo=retry(partial(setup_queue, queue_name_output))
        logging.info("Output Queue successfully created") 
    except Exception,e:
        raise Exception("Creating queue '%s': %s" % (queue_name_output, str(e)))

    # FLUSH QUEUE
    #############
    if flush_queue:
        try:    
            qi.clear()
            logging.info("Input Queue flushed")
        except: pass


    # MAIN LOOP
    ###########
    ppid=os.getppid()
    logging.info("Process pid: %s" % os.getpid())
    logging.info("Parent pid:  %s" % ppid)
    logging.info("Starting loop...")
    while True:
        if os.getppid()!=ppid:
            logging.warning("Parent terminated... exiting")
            break
        
        line=sys.stdin.readline().strip()

        if len(line)==0:
            continue

        logging.debug("Received '%s' from stdin" % line)

        if not dont_pass_through:
            stdout(line)
        
        try:
            jo=json.loads(line)
        except:
            logging.error("Can't JSON decode: %s" % line)
            continue
        
        try:
            topic=jo["topic"]
            if topic!=trigger_topic:
                logging.debug("Message trigger not found (topic was: %s)" % topic)
                continue
        except:
            logging.error("Input JSON doesn't have a 'topic' field")
            continue
                   
        try:
            if simulate_error:
                raise Exception("Network error simulation")
            
            msgs=qi.get_messages(num_messages=batch_size)
        except Exception, e:
            if log_network_error:
                logging.error("Network error - rx input queue")
            
            if error_msg is not None:
                try:    msg=error_msg % str(e)
                except: msg=error_msg
                stdout(msg)
            continue
                
        
        ### take care of sending a specified string when
        ### there are no messages in the queue
        if msgs is None or len(msgs)==0:
            if trigger_none_msg is not None:
                stdout(trigger_none_msg)
            continue
            
        ### normal flow
        for msg in msgs:
            
            try:
                b=msg.get_body()
            except:
                logging.error("Can't retrieve msg body - continue to next msg")
                if delete_on_error:
                    qi.delete_message(msg)                
                continue
            
            logging.debug("Got 1 message to process...")
            
            try:
                stdout(b, json_mode=not format_any)
            except:
                ### exit because of probable broken pipe
                raise Exception("Can't write to stdout (probably broken pipe)")
            
            ## write to output queue
            ## don't fail too fast :)
            logging.debug("Attempting to enqueue in output queue...")
            try:
                if not qo.write(msg, delay_seconds=write_delay):
                    raise
            except:
                logging.debug("Error writing to output queue -- sleeping a bit before retrying")
                sleep(1)
                try:
                    if not qo.write(msg, delay_seconds=write_delay):
                        raise
                except:
                    logging.debug("Error writing to output queue -- giving up")
                    if log_network_error:
                        logging.error("Can't write message in output queue - probable job duplication to ensue")
                continue
                
            ## After we have successfully queued the message for the output queue,
            ## delete the source message
            logging.debug("Deleting source message from input queue...")
            try:
                qi.delete_message(msg)
            except Exception, e:
                logging.debug("Error deleting source message -- sleeping a bit before retrying")
                sleep(1)
                try:
                    qi.delete_message(msg)
                except:
                    logging.debug("Error deleting source message -- giving up")
                    if log_network_error:
                        logging.error("Can't delete received msg - probable job duplication to ensue")
        


