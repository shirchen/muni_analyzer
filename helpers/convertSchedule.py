'''
Created on Feb 20, 2011

@author: shirchen

This class will convert the Muni schedule to epoch time

'''
import logging
import datetime
import time
import pymongo

class ConvertSchedule:
    
    def __init__(self):
        
        pass
    
    """
    take a line from Muni schedule formatting
    trip_id,arrival_time,departure_time,stop_id,stop_sequence,stop_headsign,pickup_type,drop_off_type,shape_dist_traveled
    4128896,09:00:00,09:00:00,3410,1, , , , 
    4128896,09:38:00,09:38:00,4603,45, , , , 


    """
    
    def parseStopsLine(self, line):
        values = line.split(',')
        if len(values) > 2:
            dep_time = values[1]
        else:
            logging.info("Could not find departure time")
            dep_time = -1
        split_time = dep_time.split(':')
        hr = int(split_time[0])
        min = int(split_time[1])
        
        return hr, min
        
    def convertToEpoch(self, line,year,month,day):
        hr, min = self.parseStopsLine(line)
        epoch_time = datetime.datetime.utcnow().replace(year=year,month=month,day=day,hour=hr,minute=min)
        int_time =  int(time.mktime(epoch_time.timetuple()))
        
        return int_time
    
    def uploadToMongo(self, stop_time):
        conn = pymongo.Connection(self.str_mongoServer)
        db = conn.muni_database
        stop_schedule = db.stop_schedule
        a_stop_time = {"route": stop_time.route,\
                       "bus_id": stop_time.bus_id,\
                       "cur_time": stop_time.cur_time,\
                       "lat": stop_time.float_lat,\
                       "lon": stop_time.float_lon,\
                       "dir": stop_time.dir_tag}
        stop_schedule.insert(a_stop_time)

        
if __name__=="__main__":
    
    LOG_FILENAME = '/tmp/Muni.out'
    logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO)