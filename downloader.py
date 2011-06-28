#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on Feb 14, 2010

@author: dima

Downloads vehicle location by latitude and longitude

Arguments:
    -t, time=    check for new data every t seconds
    -r, route=   route number

'''

import getopt
import httplib2
import logging
import os
import pymongo
import sys
import time
import xml.dom.pulldom
from main import MuniMain
from coordinates import Coordinates

'''
Class to store one line of Muni data that comes in
'''

class Downloader(MuniMain):

    def __init__(self, str_route):
# shirchen (05/16/10): I dont like what I am doing on the line below
        MuniMain.__init__(self, str_route)
        self.str_dir = os.path.join(self.str_mainDir,'location')
        
    def downloadData(self):
        
        '''
        Getting data for the past so many minutes. 0 => 15 minutes
        1144953500233 -- was tmp placeholder
        
        '''
        str_call = self.str_baseHttpCall + 'command=vehicleLocations&a=sf-muni&r=%s&t=%s'\
         % (self.str_route, '0')
        http = httplib2.Http("")
        try:
            http_resp, str_content = http.request(str_call, "GET")
            self.parseRouteData(str_content)

        except Exception, e:
            print 'Could not make call %s, got exception: %s' % (str_call, str(e))
        
    def writeDataToFile(self, coord):
        str_fileName = os.path.join(self.str_dir, self.str_route, time.strftime("%Y%m%d-%H"))        
        file_w = open(str_fileName, 'a+')    

        tmp_str_line = '\t'.join(coord.bus_id, coord.float_lat, coord.float_lon, coord.dir_tag) + '\n' 
                    
        file_w.write(tmp_str_line)
            
        file_w.close()

    def parseRouteData(self, str_data):
        doc = xml.dom.pulldom.parseString(str_data)
        str_routeDir = os.path.join(self.str_dir, self.str_route)
        if not os.path.isdir(str_routeDir):
            os.makedirs(str_routeDir)
        try:
            for event, node in doc:
                if event == 'START_ELEMENT' and node.nodeName == 'vehicle':
                    str_id = node.getAttribute("id")
                    str_routeTag = node.getAttribute("routeTag")
                    str_dirTag = node.getAttribute("dirTag")
                    str_lat = node.getAttribute("lat")
                    str_lon = node.getAttribute("lon")
                    str_leadVhclID = node.getAttribute("leadingVehicleId")
                    str_secsSinceReport = node.getAttribute("secsSinceReport")
                    
#                    logging.info("Time: %s, Id: %s, Route: %s, DirTag: %s, Lat: %s, Lon: %s,Leading ID: %s, Out of date: %s"\
#                      %(time.ctime(), str_id, str_routeTag, str_dirTag, str_lat, str_lon, str_leadVhclID,\
#                        str_secsSinceReport))
                    try:
                        coord = Coordinates(time.time(), str_id, float(str_lat), float(str_lon), str_dirTag, self.str_route)
                        self.uploadToMongo(coord)
                    except:
                        print 'Exception when creating object:' + sys.exc_info()[0]
                        raise 
                        
                     
        except Exception:
            print 'Exception:' + sys.exc_info()[0]
            raise 
    
    """
    Uploads each coordinate to MongoDB
    """
    def uploadToMongo(self, coord):
        conn = pymongo.Connection(self.str_mongoServer)
        db = conn.muni_database
        location = db.location
        coordinate = {"route": coord.route,\
                       "bus_id": coord.bus_id,\
                       "cur_time": coord.cur_time,\
                       "lat": coord.float_lat,\
                       "lon": coord.float_lon,\
                       "dir": coord.dir_tag}
        location.insert(coordinate)
        
if __name__=="__main__":
    
    LOG_FILENAME = '/tmp/Muni.out'
    logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO)
    
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hr:t:v", ["help", "route=", "time="])
    except getopt.GetoptError, err:
        # print help information and exit:
        print str(err) # will print something like "option -a not recognized"
#        usage()
        sys.exit(2)
    route = None
    verbose = False
    
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
            int_interval = int(a)
            bool_continuous = True
        else:
            assert False, "unhandled option"
    
#    str_routesSplit = str_routes.split(',')
    
    muniDownloader = Downloader(str_route)

    if bool_continuous:
        while 1:
    #        for t_str_route in str_routesSplit:
            
            muniDownloader.downloadData()
                
            time.sleep(int_interval)
            
    else:
        
        muniDownloader.downloadData()