'''
Created on Sep 26, 2011

@author: shirchen
'''
import socket
import pymongo

def connect_to_mongo():
    mongodb_local_hostname = 'ip-10-170-26-73'
    mongodb_host = 'ec2-50-18-72-59.us-west-1.compute.amazonaws.com'
    hostname = socket.gethostname()
    if hostname == mongodb_local_hostname:
        conn = pymongo.Connection()
    else:
        conn = pymongo.Connection(mongodb_host)
        
    return conn
