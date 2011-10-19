'''
Created on Sep 26, 2011

@author: shirchen
'''
import socket
import pymongo
import sys

def connect_to_mongo():
    mongodb_local_hostname = 'ip-10-170-26-73'
    mongodb_host = 'ec2-50-18-72-59.us-west-1.compute.amazonaws.com'
    hostname = socket.gethostname()
    if hostname == mongodb_local_hostname:
        try:
            conn = pymongo.Connection()
        except:
            print 'Error: ', sys.exc_info()[0]
            raise
    else:
        try:
            conn = pymongo.Connection(mongodb_host)
        except:
            print 'Error: ', sys.exc_info()[0]
            raise
    return conn
