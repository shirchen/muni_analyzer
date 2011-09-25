'''
Created on Mar 4, 2011

@author: shirchen
'''
from schedule.schedule_pull import Schedule_Pull

class Coordinates():
    
    def __init__(self, 
                 a_cur_time, 
                 a_bus_id, 
                 a_float_lat,
                 a_float_lon, 
                 a_dir_tag,
                 a_route):
        self.route = a_route
        self.cur_time = a_cur_time
        self.bus_id = a_bus_id
        self.float_lat = a_float_lat
        self.float_lon = a_float_lon
        self.dir_tag = a_dir_tag
        
import Geohash
import Coordinates
import Schedule_Pull
        
class CheckCoordinate(object):
    
    def __init__(self):
        """
        needs to be pulled
        contains a list of of trip_ids given a geohash of a (lat, lon)
        """
        self.start_point_to_trips = {}
        """
        needs to be pulled
        contains a hash of trip_id to [departure, arrival]
        """
        self.trip_to_dep_arr_times = {}
        """
        contains a hash of active bus ids to our best guess of their trip ids
        """
        self.active_bus_to_trip_id = {}
        """
        needs to be pulled
        contains a hash of trip_id to its end point
        """
        self.trip_to_end_point = {}
        """
        set of all active bus_ids
        """
        self.set_active_bus_ids = set()
        
    """
    Input:
    Output:
    Discussion:
        Lotsa mysql data pulling, hopefully will be just unpickling of objects
    """
    
    def setup_dicts(self):
        schd_pull = Schedule_Pull('22')
        schd_pull.setup_route_info()
        self.trip_to_dep_arr_times = sched_pull.retrieve_schedule()
        
    
    """
    Input: Coordinates object
    Output: A decision on whether or not the trip ended, then we add a line to 
    summary, 
    or we are starting the trip, then we add bus to list of actives, 
    or do nothing if we are in the middle of a route or at the end of the stop
    Discussion:
    """
    def check_point(self, coord):
        if coord.bus_id in self.active_bus_to_trip_id:
            """
            if our bus_id is on the move to the end
            """        
            lat = coord.float_lat
            lon = coord.float_lon
            trip_id = self.active_bus_to_trip_id[coord.bus_id]
            # TDOO: how to compare?
            (end_lat, end_lon) = self.trip_to_end_point[trip_id]
            
            if end_lat == lat and end_lon == lon:
                print 'Trip ended and adding summary line'
                self.expire_trip(coord.bus_id)
            
        else:
            """
            dont know much about bus, but lets see if we are starting a trip
            """
            trip_id = self.find_trip_id()
            self.add_to_queue(coord)
    
    """
    Input: Coodinates object
    Output: Trip_id which best approximates the trip bus is starting 
    Discussion:
        Finds the best match for a trip id, given time
        
        Logic:
            - find the first time that satisfies the following:
                * y - 120 < x and y_id is not in the queue of known buses 
    """
    def find_trip_id(self, coord):
        passive_trips = self.active_bus_to_trip_id.keys() 
        all_trips = self.trip_to_dep_arr_times.keys()
        outstanding_trips = list(set(all_trips).difference(set(passive_trips)))
        
        # sort these by time, assuming we already took care of 25:00:00
        #     awesomeness earlier
        for trip_id in outstanding_trips:
            [dep, arr] = self.trip_to_dep_arr_times[trip_id]
            cur_time = coord.cur_time
            # Now, we need to convert the schedule time into epoch time today
            # Same ole....
            # Note: we should store this, so not to have to do this every time
            dep_epoch = self.convert_time(dep)
            # Allowing buses leave up to 2 minutes early
            if dep_epoch - 120 < cur_time:
                return trip_id
        
    
    """
    Discussion:
        This function cleans up all expired trip_ids after the next day has 
        started, like keeping at most 12 bus_ids to trip_ids at a time, to
        let us know that a trip_id has already completed.
        Maybe need a separate list of all trip_ids that have already happened
        in the past 12 hours.
    """
    def expire_trip(self, bus_id):
        if bus_id in self.active_bus_to_trip_id:
            del self.active_bus_to_trip_id[bus_id]
            return True
        else:
            print '%s not in list of active buses' % (bus_id)
            #logging.debug("Error")
            return False
    
    def add_to_queue(self, coord):
        if coord.bus_id in self.set_active_bus_ids:
            return False
        else:
            self.set_active_bus_ids.add(coord.bus_id)
            return True
    
    # Helpers
    """
    Input: Time in format HH:MM:SS
    Output: That time converted into current time epoch in secs
    """
    def convert_time(self, a_time):
        time_now = time.time()
        tm_year, tm_mon, tm_mday = self.approx_date(time_now)
        return self.convert_to_epoch_time(a_time, tm_year, tm_mon, tm_mday)
    
    def convert_to_epoch_time(a_time, tm_year, tm_mon, tm_mday):
        new_datetime = datetime.datetime(*time.strptime(a_time,
                                                         '%H:%M:%S')[0:5])
        tuple = new_datetime.replace(year=tm_year, 
                                 month=tm_mon, 
                                 day=tm_mday).timetuple()
        epoch = time.mktime(tuple)
        return epoch
    
    def approx_date(epoch_secs):
        tme = time.localtime(epoch_secs)    
        return tme.tm_year, tme.tm_mon, tme.tm_mday
    