#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on Feb 14, 2010

@author: dima

Downloads vehicle location by latitude and longitude

Arguments:
    -l, logging  enable logging to /tmp/Muni.out
    -r, route=   route number
    -t, time=    check for new data every t seconds
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
from coordinates import Coordinates, CheckCoordinate

'''
Class to store one line of Muni data that comes in
'''

class Downloader(MuniMain):

    def __init__(self, route):
        MuniMain.__init__(self, route)
        self.directory = os.path.join(self.base_directory,'location')
        self.logging = False
        co = CheckCoordinate()
        co.setup_dicts()
        co.setup_trips_to_end_points()
        self.co = co

        
        
    def download_location(self):
        '''
        Getting data for the past so many minutes. 0 => 15 minutes
        1144953500233 -- was tmp placeholder
        '''
        http_location_call = self.base_http_nextbus\
         + 'command=vehicleLocations&a=sf-muni&r=%s&t=%s'\
         % (self.route, '0')
        http = httplib2.Http("")
        try:
            http_resp, content = http.request(http_location_call, "GET")
            if http_resp['status'] == '200':
                self.parse_location_data(content)
            else:
                print 'Response not 200'
                logging.info('Response not 200')

        except Exception, e:
            print 'Could not make call %s, got exception: %s' % (http_location_call, str(e))
            try:
                print "Response: %s, content: %s" %(http_resp, content)
            except:
                pass
    """
    At first, I was storing data into files, but now MongoDB is used 
    """    
    def writeDataToFile(self, coord):
        route_directory = os.path.join(self.directory, self.route)
        if not os.path.isdir(route_directory):
            os.makedirs(route_directory)

        str_fileName = os.path.join(self.directory, self.route,
                                     time.strftime("%Y%m%d-%H"))        
        file_w = open(str_fileName, 'a+')    
        line = '\t'.join(coord.bus_id, coord.float_lat, coord.float_lon,
                                  coord.dir_tag) + '\n' 
        file_w.write(line)
        file_w.close()

    def parse_location_data(self, xml_response):
        doc = xml.dom.pulldom.parseString(xml_response)
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
                    speed = node.getAttribute("speedKmHr")
                    if self.logging:
                        logging.info("Time: %s, Id: %s, Route: %s, DirTag: %s,"
                         "Lat: %s, Lon: %s,Leading ID: %s, Out of date: %s"\
                          %(time.ctime(), str_id, str_routeTag, str_dirTag,
                             str_lat, str_lon, str_leadVhclID,
                              str_secsSinceReport))
                    try:
                        coord = Coordinates(time.time(), str_id, 
                                            float(str_lat), float(str_lon),
                                             str_dirTag, self.route, speed)
                        try:
                            self.co.check_point(coord)
                        except:
                            print 'Failed to check coordinate: %s'\
                            % (str(coord))
                        """
                        Put back when we are actually collecting data
                        """
#                        self.uploadToMongo(coord)
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
#        db = conn.muni_database # picking muni_database db
        db = conn.muni # picking muni db
        location = db.location  # picking the collection to insert data into
#        coordinate = {"route": coord.route,\
#                       "bus_id": coord.bus_id,\
#                       "cur_time": coord.cur_time,\
#                       "lat": coord.float_lat,\
#                       "lon": coord.float_lon,\
#                       "dir": coord.dir_tag}
        coordinate = {"route": coord.route,
                       "bus_id": coord.bus_id,
                       "cur_time": coord.cur_time,
                       "loc":{"lat":coord.float_lat,"lon": coord.float_lon},
                       "dir": coord.dir_tag,
                       "speed": coord.speed}
        location.insert(coordinate)
        
    def tests(self):
        h = httplib2.Http("")
        resp, content = h.request("http://ec2-50-18-72-59.us-west-1.compute.amazonaws.com:3000/test_xml_first.xml", "GET")
        if resp['status'] == '200':
            self.parse_location_data(content)
        else:
            print 'Response not 200 for first'
            
        resp, content = h.request("http://ec2-50-18-72-59.us-west-1.compute.amazonaws.com:3000/test_xml_second.xml", "GET")
        if resp['status'] == '200':
            self.parse_location_data(content)
        else:
            print 'Response not 200 for second'
            
        resp, content = h.request("http://ec2-50-18-72-59.us-west-1.compute.amazonaws.com:3000/test_xml_last.xml", "GET")
        if resp['status'] == '200':
            self.parse_location_data(content)
        else:
            print 'Response not 200 for last'

# Helpers
def usage():
    print """
Arguments:
    -l, logging  enable logging to /tmp/Muni.out
    -r, route=   route number
    -t, time=    check for new data every t seconds
    """
        
if __name__=="__main__":
    LOG_FILENAME = '/tmp/Muni.out'
    logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO)
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hlp:r:t:uv",
                                    ["help", "logging", "period=", "route=",
                                      "time=", "unittest"])
    except getopt.GetoptError, err:
        # print help information and exit:
        print "Error: " + str(err) # will print something like "option -a not recognized"
        usage()
        sys.exit(2)
    route = None
    verbose = False
    continuous = False
    to_log = False
    unit_test = False
    num_hours_to_collect = 0
    time_at_start = time.time()
    for o, a in opts:
        if o == "-v":
            verbose = True
        elif o in ("-h", "--help"):
#            usage()
            sys.exit()
        elif o in ("-p", "--period"):
            num_hours_to_collect = float(a)    
        elif o in ("-l", "--logging"):
            to_log = True
        elif o in ("-r", "--route"):
            route = a
        elif o in ("-t", "--time"):
            int_interval = int(a)
            continuous = True
        elif o in ("-u", "--unittest"):
            unit_test = True
        else:
            assert False, "unhandled option"
    muniDownloader = Downloader(route)
    muniDownloader.logging = to_log
    if unit_test:
        muniDownloader.tests()
    else:
        if continuous and num_hours_to_collect == 0:
            while 1:
                muniDownloader.download_location()
                time.sleep(int_interval)
        elif num_hours_to_collect > 0:
            num_secs_to_collect = num_hours_to_collect * 3600
            stoppage_time = time_at_start+num_secs_to_collect
            print "Collecting until %s" % (time.ctime(stoppage_time))
            while time.time() < stoppage_time:
                muniDownloader.download_location()
                time.sleep(int_interval)
        else:
            muniDownloader.download_location()