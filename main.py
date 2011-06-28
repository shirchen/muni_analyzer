'''
Created on Feb 22, 2010

@author: dima

Main class for Muni package
TODO: Create a config which will run the first time and will write
    default directory the first time and save it into a config file. 
'''

import os

class MuniMain(object):
    
    def __init__(self, str_route):
        self.str_route = str_route
#        self.str_mainDir = '/mnt/ashkelon/muni'
        self.str_mainDir = '/Users/shirchen/muni'
        self.str_baseHttpCall = 'http://webservices.nextbus.com/service/publicXMLFeed?'
        self.str_mongoServer = 'ec2-50-16-77-90.compute-1.amazonaws.com'
    
    def setup(self):

        """
            Setup directory structure
        """
        if not os.path.isdir(self.str_mainDir):
            os.makedirs(self.str_mainDir)
        
