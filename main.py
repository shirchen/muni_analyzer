'''
Created on Feb 22, 2010

@author: dima

Main class for Muni package
TODO: Create a config which will run the first time and will write
    default directory the first time and save it into a config file. 
'''

import os

class MuniMain(object):
    
    def __init__(self, route):
        self.route = route
        self.base_directory = os.path.expanduser("~/muni")
        self.base_http_nextbus = 'http://webservices.nextbus.com/service/publicXMLFeed?'
        self.str_mongoServer = 'ec2-50-18-72-59.us-west-1.compute.amazonaws.com'

    
    def setup(self):

        """
            Setup directory structure
        """
        if not os.path.isdir(self.base_directory):
            os.makedirs(self.base_directory)
        
