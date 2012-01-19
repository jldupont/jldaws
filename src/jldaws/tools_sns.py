"""
    Created on 2012-01-19
    @author: jldupont
"""
import os

def build_topic_arn(conn, topic):
    account_id=os.environ["AWS_ACCOUNT_ID"]
    return 'arn:aws:sns:%s:%s:%s' % (conn.region.name, account_id, topic)
