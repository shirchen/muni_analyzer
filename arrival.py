#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on Feb 16, 2010

@author: dima

Downloads next's arrival predictions

Logic:
    Get all stops for a route
    pass each stop into the http call
    parse the returned data

'''
from main import MuniMain
from route import Route
import getopt
import httplib2
import logging
import os
import sys
import time
import xml.dom.pulldom

class Arrival(MuniMain):
    
    
    def __init__(self, str_route):
# shirchen (05/16/10): I dont like what I am doing on the line below
        MuniMain.__init__(self, str_route)
        self.str_dir = os.path.join(self.base_directory, 'arrival')
        self.l_stops = []
        self.dict_titleToStop = {}

    def downloadData(self):
        
        """
            Load dict with route mappings into memory
            and run predictions for all stops defined on a route
        """
        
        str_call = self.base_http_nextbus +'command=predictionsForMultiStops&a=sf-muni' 
        
        for str_stop in self.l_stops:
            str_call = str_call + '&stops=%s|null|%s' % (self.str_route, str_stop)
            
#            &stops=%s|null|6997&stops=%s|null|3909' \
#                    % (self.str_route, self.str_route)
        http = httplib2.Http("")
    
        # TODO: what if connection goes bad?
    
        http_resp, str_content = http.request(str_call, "GET")
        
        self.parseArrivalData(str_content)
    
    def parseArrivalData(self, str_content):
        doc = xml.dom.pulldom.parseString(str_content)
        
        str_fileName = os.path.join(self.str_dir, self.str_route, time.strftime("%Y%m%d-%H"))
        str_routeDir = os.path.join(self.str_dir, self.str_route)
        if not os.path.isdir(str_routeDir):
            os.makedirs(str_routeDir)
        
        file_w = open(str_fileName, 'a+')
        
        int_index = 0
        
        try:
            for event, node in doc:
                if event == 'START_ELEMENT' and node.nodeName == 'predictions':
                    str_title = node.getAttribute("stopTitle")
                    str_stop = self.dict_titleToStop[str_title]
                elif event == 'START_ELEMENT' and node.nodeName == 'prediction':
                    str_secs = node.getAttribute("seconds")
                    str_mins = node.getAttribute("minutes")
                    str_epochTime = node.getAttribute("epochTime")
                    str_isDeparture = node.getAttribute("isDeparture")
                    str_dirTag = node.getAttribute("dirTag")
                    str_vehicleID = node.getAttribute("vehicle")
                    str_block = node.getAttribute("block")
                    
                    logging.info("Time: %s, Route: %s, Stop: %s, Seconds: %s, Minutes: %s, epochTime: %s, isDeaprture: %s, dirTag: %s, Vehicle: %s,Block: %s"\
                      %(time.ctime(), self.str_route, str_stop, str_secs, str_mins, str_epochTime, str_isDeparture, str_dirTag, str_vehicleID,\
                        str_block))
                     
                    t_str_line = str(time.time()) + '\t' + str_stop + '\t' + str_secs + '\t' + str_mins + '\t' + str_epochTime + '\t' \
                    + str_isDeparture + '\t' + str_dirTag + '\t' + str(str_vehicleID) + '\t' + str(str_block) +'\n'
                    
                    file_w.write(t_str_line)
                elif event == 'END_ELEMENT' and node.nodeName == 'predictions':
                    str_stop = '' 
            
            file_w.close()
        except:
            print 'Exception'
        
    def loadTitleToStopDict(self):
        route = Route(self.str_route)
        self.dict_titleToStop = route.loadTitleToStopDict()
    
    def loadStops(self):
        
        route = Route(self.str_route)
#        dict_stops = route.loadStopsDict()
        dict_routes = route.loadRoutesDict()
        
        set_allStops = set()

        for str_routeAbb in dict_routes.keys():
            set_allStops = set_allStops.union(dict_routes[str_routeAbb])
            
        self.l_stops = list(set_allStops)
        
#        for str_routeAbb in dict_routes.keys():
#            print str_routeAbb
#            print dict_routes[str_routeAbb]
#            self.l_stops = dict_routes[str_routeAbb]    
#    
#        for str_stop in dict_stops.keys():
#            print str_stop
#            print dict_stops[str_stop]
        
        
    def setup(self):
        
        if not os.path.isdir(self.str_dir):
            os.makedirs(self.str_dir)
    
if __name__=="__main__":
    
        
    LOG_FILENAME = '/tmp/arrival.out'
    logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO)
    
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hr:t:", ["help", "route=","time="])
    except getopt.GetoptError, err:
        print str(err)
        sys.exit(2)
    
    bool_continuous = False
    
    for o, a in opts:
        if o == "-v":
            verbose = True
        elif o in ("-h", "--help"):
#            usage()
            sys.exit()
        elif o in ("-r", "--route"):
            str_route = a
        elif o in ("-t", "--time"):
            bool_continuous = True
            int_interval = int(a)
        else:
            assert False, "unhandled option"
    
    arrival = Arrival(str_route)
    arrival.loadStops()
    arrival.loadTitleToStopDict()
    
    if bool_continuous:
        while 1:
            arrival.downloadData()
            time.sleep(int_interval)
    else:
        arrival.downloadData()
        
    
    