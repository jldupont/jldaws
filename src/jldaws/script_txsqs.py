"""
    Created on 2012-01-19
    @author: jldupont
"""
import logging, json, sys, os
from   time import sleep
import boto

from boto.sqs.jsonmessage import JSONMessage
from boto.sqs.message     import RawMessage

from tools_sys     import retry


def stdout(s):
    try:
        if not s.endswith("\n"):
            s=s+"\n"
    except:
        pass
    sys.stdout.write(s)
    sys.stdout.flush()

def run(queue_name=None,
        flush_queue=None,
        format_any=None,
        retry_always=None,
        topics=None,
        error_msg=None,
        simulate_error=None,
        **_
        ):
    
    queue_name=queue_name.strip()
    
    # SETUP PRIVATE QUEUE
    def setup_private_queue():
        conn = boto.connect_sqs()
        q=conn.create_queue(queue_name)
        if not format_any:
            q.set_message_class(JSONMessage)
        else:
            q.set_message_class(RawMessage)
        return q
        
    try:
        q=retry(setup_private_queue, always=retry_always)
    except Exception,e:
        raise Exception("Creating queue '%s': %s" % (queue_name, str(e)))

    if flush_queue:
        try:    
            q.clear()
            logging.info("queue flushed")
        except: pass

    ppid=os.getppid()
    logging.debug("Starting loop...")
    logging.info("Process pid: %s" % os.getpid())
    logging.info("Parent pid:  %s" % ppid)
    while True:
        if os.getppid()!=ppid:
            logging.warning("Parent terminated... exiting")
            break
        line=sys.stdin.readline()
        
        ## echo on stdout
        stdout(line)
        
        ## by default, allow the message to pass to the queue
        allow_msg=True
        if not format_any:
            try:
                jo=json.loads(line)
            except:
                logging.error("Can't load json object from: %s" % line)
                continue
            
            if topics is not None:
                try:
                    msg_topic=jo["topic"]
                except:
                    logging.error("Can't extract 'topic' field from %s: " % line)
                    continue
                try:
                    allow_msg=any(map(lambda s:msg_topic.startswith(s), topics))
                except Exception,e:
                    logging.error("Error processing message topic against list of topics: %s" % e)
                    continue
            
            if not allow_msg:
                continue
            
            m=JSONMessage()
            m.set_body(jo)
        else:
            m=RawMessage()
            m.set_body(line)
        
        logging.debug("Writing to SQS queue '%s': %s" % (queue_name, line))
        try:
            if simulate_error:
                raise Exception("Network error simulation")
            q.write(m)
        except:
            ### give us a chance...
            sleep(1)
            try:
                if simulate_error:
                    raise Exception("Network error simulation - 2nd attempt")                
                q.write(m)
            except Exception,e:
                if retry_always:
                    logging.error("Can't write to SQS queue - 2nd attempt in a row: %s" % e)
                else:
                    if error_msg is not None:
                        stdout(error_msg)
                    else:
                        raise Exception("Writing to SQS queue")

        
