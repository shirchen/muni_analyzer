#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on Feb 21, 2010

@author: dima

Downloads information about a specified route as a dict
and serializes on the file system

'''

import cPickle
import getopt
import httplib2
import logging
import os
import sys
import xml.dom.pulldom
from main import MuniMain

class Route(MuniMain):
    
    def __init__(self, str_route):
# shirchen (05/16/10): I dont like what I am doing on the line below
        MuniMain.__init__(self, str_route)
        self.str_dir = os.path.join(self.str_mainDir, 'routes')
        self.str_routeDir = os.path.join(self.str_dir, self.str_route)
        self.str_stopCoordFile = os.path.join(self.str_routeDir, 'stopCoordinates')
        self.str_dictRoutesFile = os.path.join(self.str_routeDir, 'dictRoutes')
        self.str_dictTitleToStopFile = os.path.join(self.str_routeDir, 'titleToStop')
    
    def openRoute(self, str_content):
        
        doc = xml.dom.pulldom.parseString(str_content)
        dict_stops = {}
        dict_routes = {}
        dict_titleToStop = {}
        for event, node in doc:
            if event == 'START_ELEMENT' and node.nodeName == 'stop' and node.getAttribute('lat') != '':
                str_tag = node.getAttribute('tag')
                
                str_lat = node.getAttribute('lat')
                str_lon = node.getAttribute('lon')
                
                str_title = node.getAttribute('title')
                # shirchen (06/05/2010) maybe make floats before storing
                dict_stops[str_tag] = [str_lat, str_lon]
                
                # shirchen (05/16/2010) Make tag the key and title value ie swap those
                dict_titleToStop[str_title] = str_tag
                
                logging.info('Tag: %s, Lat: %s, Lon: %s, Title: %s', str_tag, str_lat, str_lon, str_title)
                
            elif event == 'START_ELEMENT' and node.nodeName == 'direction':
                str_tag = node.getAttribute('tag')
                t_l_tags = []
                dict_routes[str_tag] = t_l_tags
            elif event == 'END_ELEMENT' and node.nodeName == 'direction':
                t_l_tags = []
            elif event == 'START_ELEMENT' and node.nodeName == 'stop' and node.getAttribute('lat') == '':
                str_tag = node.getAttribute('tag')
                t_l_tags.append(str_tag)
        
        if not os.path.isdir(os.path.split(self.str_stopCoordFile)[0]):
            os.makedirs(os.path.split(self.str_stopCoordFile)[0])
                
        file_obj = open(self.str_stopCoordFile, 'wb')
        cPickle.dump(dict_stops, file_obj)
        file_obj.close()
        
        file_obj = open(self.str_dictRoutesFile, 'wb')
        cPickle.dump(dict_routes, file_obj)
        file_obj.close()
        
        file_obj = open(self.str_dictTitleToStopFile, 'wb')
        cPickle.dump(dict_titleToStop, file_obj)
        file_obj.close()

    """
        Format:
            stopID to [longitude, latitude]
             {u'4608': [u'37.80109', u'-122.43623'], u'4609': [u'37.8008899',
                                                              u'-122.43647'],.
    """
    def loadStopsDict(self):
        
        if not os.path.isfile(self.str_stopCoordFile):
            self.getRawRoute()
        
        file_obj = open(self.str_stopCoordFile, 'rb')
        dict_stops = cPickle.load(file_obj)
        file_obj.close()
        
        return dict_stops
    
    """
    Format:
        mapping of routeId to list of stops  
    {u'22_IB2': [u'3410', u'6657', u'3342', u'3346', u'3324', u'4123',...]
    """
    def loadRoutesDict(self):
        
        if not os.path.isfile(self.str_dictRoutesFile):
            self.getRawRoute()
        
        file_obj = open(self.str_dictRoutesFile, 'rb')
        dict_routes = cPickle.load(file_obj)
        file_obj.close()
        
        return dict_routes
    
    """
        Format:
            mapping of title to stopID
         {u'Fillmore St & Jefferson St': u'4625', 
          u'Church St & Market St': u'7073'
    
    """
    def loadTitleToStopDict(self):
        
        if not os.path.isfile(self.str_dictTitleToStopFile):
            self.getRawRoute()
        
        file_obj = open(self.str_dictTitleToStopFile, 'rb')
        dict_routes = cPickle.load(file_obj)
        file_obj.close()
        
        return dict_routes

    
    def getRawRoute(self):
        
        str_call = self.str_baseHttpCall + 'command=routeConfig&a=sf-muni&r=' + self.str_route
        
        http = httplib2.Http("")
        http_resp, str_content = http.request(str_call, "GET")
        print str_content
        self.openRoute(str_content)
        
    def setup(self):
        
        if not os.path.isdir(self.str_dir):
            os.makedirs(self.str_dir)
            
        if not os.path.isdir(self.str_routeDir):
            os.makedirs(self.str_routeDir)
        
if __name__=="__main__":
    
    LOG_FILENAME = '/tmp/routes.out'
    logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO)
    
    try:
        opts, args = getopt.getopt(sys.argv[1:], "ghr:", ["get","help", "route=",])
    except getopt.GetoptError, err:
        print str(err)
        sys.exit(2)
        
    for o, a in opts:
        if o == "-v":
            verbose = True
        elif o in ("-h", "--help"):
#            usage()
            sys.exit()
#        elif o in ("-g", "--get"):
#            str_route = a
        elif o in ("-r", "--route"):
            str_route = a

        else:
            assert False, "unhandled option"
            
    route = Route(str_route)
    route.setup()
    route.getRawRoute()        
