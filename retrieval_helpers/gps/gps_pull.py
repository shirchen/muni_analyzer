'''
Created on Sep 21, 2011

@author: shirchen

Thic class is responsible for pulling GPS data
'''
import pymongo
import socket
from pymongo import ASCENDING, DESCENDING
from collections import defaultdict

class GPS_Pull(object):
    
    def __init__(self, route_name):
        self.end_bus_ids_to_times = defaultdict(list)
        self.mongodb_host = 'ec2-50-18-72-59.us-west-1.compute.amazonaws.com'
        self.mongodb_local_hostname = 'ip-10-170-26-73'
        self.route_name = route_name 
        self.start_bus_ids_to_times = defaultdict(list)
        self.to_log = True
        self.LEAST_TIME_TO_DO_ROUNDTRIP = 50*60
        
    def setup_route_info(self):
        if self.route_name == '22':
            self.route_id = "6011"
            self.service_id = "1"
            self.direction_id = "1"
            self.first_stop = "1"
            self.last_stop = "45"
            self.mysql_data = {'route_id':"6011", 'service_id':"1",
                                'direction_id':"1", 'first_stop':"1",
                                 'last_stop':"45"}
            
            self.start_lat, self.start_lon, self.start_prec =\
             37.76048, -122.38895, 0.002
            self.end_lat, self.end_lon, self.end_prec =\
             37.80241, -122.4364, 0.001
        elif self.route_name == 'J':
            self.mysql_data = {'route_id':"1094", 'service_id':"1"}
        elif self.route_name == '14':
            self.mysql_data = {'route_id':"6005", 'service_id':"1",
                               'direction_id':"1",'first_stop':"1",
                               'last_stop':"51"}
            self.start_lat, self.start_lon, self.start_prec =\
             37.706, -122.461, 0.002
            self.end_lat, self.end_lon, self.end_prec =\
             37.7932, -122.393, 0.002
        
    def connect_to_mongo(self):
        hostname = socket.gethostname()
        if hostname == self.mongodb_local_hostname:
            conn = pymongo.Connection()
        else:
            conn = pymongo.Connection(self.mongodb_host)
            
        return conn
    
    def massage_start_data(self, mongo_cursor):
        prev_timestamp = 2**50
        for line in mongo_cursor:
            bus_id = line['bus_id']
            timestamp = line['cur_time']
            if timestamp < prev_timestamp - self.LEAST_TIME_TO_DO_ROUNDTRIP:
                self.start_bus_ids_to_times[bus_id].append(timestamp)
                prev_timestamp = timestamp
        return self.start_bus_ids_to_times
      
    def massage_end_data(self, mongo_cursor):
        prev_timestamp = 0
        for line in mongo_cursor:
            bus_id = line['bus_id']
            timestamp = line['cur_time']
            if timestamp > prev_timestamp + self.LEAST_TIME_TO_DO_ROUNDTRIP:
                self.end_bus_ids_to_times[bus_id].append(timestamp)
                prev_timestamp = timestamp
        return self.end_bus_ids_to_times
    
    def get_data(self):
        """
        Extracts GPS coordinates from MongoDB
        Input: null
        Output: a processed dictionary with dictionary with bus_id -> [t_0, t_1]
            mapping
        """
#        epoch_from = 1301641200
#        epoch_to = epoch_from+60*60*24
        """
         letting runs finish for 2 more hours
         ideally, want to make this a function of time from schedule plus some
        variation, like 1 hour just in case
        """ 
#        epoch_to_adjusted = epoch_to + 7200
        conn = self.connect_to_mongo()
        db = conn.muni
        
#        print "==== Collecting starting runs from %s to %s ===="\
#         % (str(time.ctime(epoch_from)), str(time.ctime(epoch_to)))
        """
        > db.location.find({loc:{$within:{$center:[[37.80241, -122.4364],
        0.01]}}})
        > db.location.find({loc:{$within:{$center:[[37.76048, -122.38895],
        0.002]}}})
        """
        bus_ids = db.location.find({'route':self.route_name}).distinct("bus_id")
        for bus_id in bus_ids:
            c_start = db.location.find({"bus_id":bus_id,
                                        "loc":{"$within":{"$center":[[self.start_lat, self.start_lon],
                                                                     self.start_prec]}}
                                        }).sort("cur_time", DESCENDING)
            self.massage_start_data(c_start)
            """
            TODO: the end point seems to be too nice to Muni, need to tighten
            the circle a little
            """
            c_end = db.location.find({"bus_id":bus_id,
                                      "loc":{"$within":{"$center":[[self.end_lat, self.end_lon],
                                                                   self.end_prec]}}
                                      }).sort("cur_time", ASCENDING)
            self.massage_end_data(c_end)
        if self.to_log:
            print self.start_bus_ids_to_times
            print self.end_bus_ids_to_times
        
        return self.start_bus_ids_to_times, self.end_bus_ids_to_times
    
def tests():
    pass


if __name__ == "__main__":
    tests()
    gps_pull = GPS_Pull('22')
    gps_pull.setup_route_info()
    gps_pull.get_data()
