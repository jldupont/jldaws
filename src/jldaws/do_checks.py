"""
    Created on 2012-01-19
    @author: jldupont
"""
import os

try:
    import boto
except:
    raise Exception("* package 'boto' is required - get it from Pypi\n")

try:
    import argparse
except:
    raise Exception("* package 'argparse' is necessary - get it from Pypi\n")

try:
    os.environ["AWS_ACCESS_KEY_ID"]
    os.environ["AWS_SECRET_ACCESS_KEY"]
    os.environ["AWS_ACCOUNT_ID"]
except Exception, e:
    raise Exception("* environment variable missing: %s" % str(e))
