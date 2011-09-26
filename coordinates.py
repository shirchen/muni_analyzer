'''
Created on Mar 4, 2011

@author: shirchen
'''
import datetime
import time
from schedule import schedule_pull
from analyze_data import Output

class Coordinates():
    
    def __init__(self, 
                 a_cur_time, 
                 a_bus_id, 
                 a_float_lat,
                 a_float_lon, 
                 a_dir_tag,
                 a_route,
                 a_speed):
        self.route = a_route
        self.cur_time = a_cur_time
        self.bus_id = a_bus_id
        self.float_lat = a_float_lat
        self.float_lon = a_float_lon
        self.dir_tag = a_dir_tag
        self.speed = a_speed
        
import Geohash
#import Coordinates
#import Schedule_Pull
        
class CheckCoordinate(object):
    
    def __init__(self):
        """
        needs to be pulled
        contains a list of of trip_ids given a geohash of a (lat, lon)
        """
        self.start_point_to_trips = {} #Could hardcode for 22
        """
        needs to be pulled
        contains a hash of trip_id to [departure, arrival]
        """
        self.trip_to_dep_arr_times = {} #Done
        """
        contains a hash of active bus ids to our best guess of their trip ids
        """
        self.active_bus_to_trip_id = {} #Local
        """
        needs to be pulled
        contains a hash of trip_id to its end point
        """
        self.trip_to_end_point = {} #Could hardcode for 22
        """
        set of all active bus_ids
        """
        self.set_active_bus_ids = set() #Local
    
    """
    Input: Route_name eg '22'
    Output: Dictionary which maps all of the trips associated with that route 
        to a gps coordinate where the trips end  
    Discussion: This guy is needed so that we when we check if a certain bus_id
        has come to its destination. So, upon receiving a new coordinate object,
        we see if the we have reached the end
    TODO:
        This should live in schedule puller class
    """
    def setup_trips_to_end_points(self):
        for trip_id in self.trip_to_dep_arr_times.keys():
            self.trip_to_end_point[trip_id] = [37.80241, -122.4364]
            
    def setup_start_points_to_trip(self):
        """
        Input: Route_name eg '22'
        Output: Dictionary which maps all of the trips associated with that route 
            to a gps coordinate where the trips start  
        Discussion: 
        TODO:
            This should live in schedule puller class
    
        """
        for trip_id in self.trip_to_dep_arr_times.keys():
            self.start_point_to_trips[trip_id] = Geohash.encode(37.76048,
                                                                 -122.38895)
            pass
#            self.trip_to_end_point[trip_id] = [37.80241, -122.4364]


    
    def setup_dicts(self):
        """
        Input:
        Output:
        Discussion:
            Lotsa mysql data pulling, hopefully will be just unpickling of objects
        """
        
        schd_pull = schedule_pull.Schedule_Pull('22')
        schd_pull.setup_route_info()
        self.trip_to_dep_arr_times = schd_pull.retrieve_pysql_schedule()
    
    def check_point(self, coord):
        """
        Input: Coordinates object
        Output: A decision on whether or not the trip ended, then we add a line to 
        summary, 
        or we are starting the trip, then we add bus to list of actives, 
        or do nothing if we are in the middle of a route or at the end of the stop
        Discussion:
        """
        if coord.bus_id in self.active_bus_to_trip_id:
            """
            if our bus_id is on the move to the end
            """        
            lat = coord.float_lat
            lon = coord.float_lon
            trip_id = self.active_bus_to_trip_id[coord.bus_id][0]
            # TDOO: how to compare?
            (end_lat, end_lon) = self.trip_to_end_point[trip_id]
            
            ### TODO: relax exact location
#            if end_lat == lat and end_lon == lon:
            if self.check_if_point_is_nearby(lat, lon, end_lat, end_lon):
                print 'Trip ended and adding summary line'
                self.upload_summary_to_mongo(coord)
                self.expire_trip(coord.bus_id)
                print 'Finished'
        else:
            """
            First check if we are starting a trip
            """
            if self.check_if_starting_trip(coord):
                """
                Then we look for a trip id associated with it
                """
                trip_id = self.find_trip_id(coord)
                self.add_to_queue(coord, trip_id)
                print 'Adding trip to queue'
    
    def find_trip_id(self, coord):
        """
        Input: Coordinates object
        Output: Trip_id which best approximates the trip bus is starting 
        Discussion:
            Finds the best match for a trip id, given time
            
            Logic:
                - find the first time that satisfies the following:
                    * y - 120 < x and y_id is not in the queue of known buses 
        """
        passive_trips = self.active_bus_to_trip_id.keys() 
        all_trips = self.trip_to_dep_arr_times.keys()
        outstanding_trips = list(set(all_trips).difference(set(passive_trips)))
        
        # sort these by time, assuming we already took care of 25:00:00
        #     awesomeness earlier
        for trip_id in outstanding_trips:
            (dep, arr) = self.trip_to_dep_arr_times[trip_id]
            cur_time = coord.cur_time
            # Now, we need to convert the schedule time into epoch time today
            # Same ole....
            # Note: we should store this, so not to have to do this every time
            dep_epoch = self.convert_time(dep)
            # Allowing buses leave up to 2 minutes early
            if dep_epoch - 120 < cur_time:
                return trip_id
    
    def expire_trip(self, bus_id):
        """
        Discussion:
            This function cleans up all expired trip_ids after the next day has 
            started, like keeping at most 12 bus_ids to trip_ids at a time, to
            let us know that a trip_id has already completed.
            Maybe need a separate list of all trip_ids that have already happened
            in the past 12 hours.
        """
        if bus_id in self.active_bus_to_trip_id:
            del self.active_bus_to_trip_id[bus_id]
            return True
        else:
            print '%s not in list of active buses' % (bus_id)
            #logging.debug("Error")
            return False
    
    def add_to_queue(self, coord, trip_id):
        if coord.bus_id in self.set_active_bus_ids:
            return False
        else:
            self.set_active_bus_ids.add(coord.bus_id)
            self.active_bus_to_trip_id[coord.bus_id] = [trip_id, time.time()]
            return True
    
    # Helpers
    def convert_time(self, a_time):
        """
        Input: Time in format HH:MM:SS
        Output: That time converted into current time epoch in secs
        """
        time_now = time.time()
        tm_year, tm_mon, tm_mday = self.approx_date(time_now)
        return self.convert_to_epoch_time(a_time, tm_year, tm_mon, tm_mday)
    
    def convert_to_epoch_time(self, a_time, tm_year, tm_mon, tm_mday):
        new_datetime = datetime.datetime(*time.strptime(a_time,
                                                         '%H:%M:%S')[0:5])
        tuple = new_datetime.replace(year=tm_year, 
                                 month=tm_mon, 
                                 day=tm_mday).timetuple()
        epoch = time.mktime(tuple)
        return epoch
    
    def approx_date(self, epoch_secs):
        tme = time.localtime(epoch_secs)
        return tme.tm_year, tme.tm_mon, tme.tm_mday
    
    def check_if_point_is_nearby(self, lat, lon, end_lat, end_lon):
        """
        Checking to see if we are within the square. Maybe we should make this into 
            a circle.
        """
        if end_lat-0.001 < lat < end_lat+0.001\
        and end_lon-0.001 < lon < end_lon+0.001:
            return True
        else:
            return False
    
    def check_if_starting_trip(self, coord):
        """
        What can we use here? This is kinda fun. We could check to see if the
             bus has a velocity, and it is in a square which we drew.
        
        Then, the quesiton of how to hash the coordinates and pull them out 
            again. For now, we are gonna hack it with hardcoded values for the 
            '22'.
        """
        (lat, lon) = (37.76048, -122.38895)
        if coord.speed != 0\
         and lat-0.001 < coord.float_lat < lat+0.001\
         and lon-0.001 < coord.float_lon < lon+0.001:
            return True
        else:
            return False
        
    def upload_summary_to_mongo(self, coord):
        # need to define
        start_time = ''
        (trip_id, start_time) = self.active_bus_to_trip_id[coord.bus_id]
        (schd_start, schd_end) = self.trip_to_dep_arr_times[trip_id]
        schd_start = self.convert_time(schd_start)
        schd_end = self.convert_time(schd_end)
        end_time = time.time()
#        out = Output()
        out = Output(coord.route,
                     trip_id,
                     coord.bus_id,
                     start_time,
                     end_time,  #end time is now
                     end_time-schd_end,
                     schd_start,
                     schd_end,
                     start_time-schd_start
                     )
        print out
        """
            def __init__(self, 
                 route_name,
                 trip_id,
                 bus_id,
                 start_time,
                 end_time,
                 secs_late,
                 schd_start,
                 schd_end,
                 leaving_late
                 ):
        
        
        
                self.route = a_route
        self.cur_time = a_cur_time
        self.bus_id = a_bus_id
        self.float_lat = a_float_lat
        self.float_lon = a_float_lon
        self.dir_tag = a_dir_tag
        self.speed = a_speed
        
            def to_mongo(self):
        row = {"route_name" :self.route_name,
               "trip_id": self.trip_id,
               "bus_id" : self.bus_id,
               "start_time": self.start_time,
               "end_time": self.end_time,
               "secs_late" : self.secs_late,
               "schd_start": self.schd_start,
               "schd_end" : self.schd_end,
               "leaving_late" : self.leaving_late,
               }
        return row
        """

def tests():
    co = CheckCoordinate()
    first_coord = Coordinates(time.time(), 5432, 37.76048, -122.38895, "22_IB2", "22", '20.988')
    last_coord = Coordinates(time.time(), 5432, 37.80241, -122.4364, "22_IB2", "22", '10.988')
    co.setup_dicts()
    co.setup_trips_to_end_points()
    co.check_point(first_coord)
    co.check_point(last_coord)
    """
    Then throw a point which is almost finished in a few seconds for same
        object
    """
        
if __name__=="__main__":
    tests()
    