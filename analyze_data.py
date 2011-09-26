#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on Jun 30, 2011

@author: dima

Analyzes accumulated Muni data
 
Arguments:

TODOs:
    - measure special routes which do not go all the way
    - measure both directions
    - expand to support all routes
'''

import datetime
import getopt
import logging
import os
import pymongo
import socket
import sys
import time
import _mysql
from collections import defaultdict
from pymongo import ASCENDING, DESCENDING

class Output(object):
    
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
        self.route_name = route_name
        self.trip_id = trip_id
        self.bus_id = bus_id
        self.start_time = start_time
        self.end_time = end_time
        self.secs_late = secs_late
        self.schd_start = schd_start
        self.schd_end = schd_end
        self.leaving_late = leaving_late
        
    def __str__(self):
        return "route name: %s, "\
            "trip_id: %s, "\
            "bus_id: %s, "\
            "start_time: %s, "\
            "end_time: %s, "\
            "secs_late: %s, "\
            "schd_start: %s, "\
            "schd_end: %s, "\
            "leaving_late: %s"\
     % (self.route_name,
         self.trip_id,
         self.bus_id,
         self.start_time,
         self.end_time,
         self.secs_late,
         self.schd_start,
         self.schd_end,
         self.leaving_late)
        
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

class Analyze(object):
    mysql_password = None
    

    def __init__(self, route_name):
        self.to_log = False
        self.mysql_host = "ec2-50-18-72-59.us-west-1.compute.amazonaws.com"
        self.mysql_local_hostname = 'ip-10-170-26-73'
        self.start_bus_ids_to_times = defaultdict(list)
        self.end_bus_ids_to_times = defaultdict(list)
        self.LEAST_TIME_TO_DO_ROUNDTRIP = 50*60
        self.route_name = route_name
    
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

        
    def get_mysql_password(self):
        file = open(os.path.expanduser("~/.mysql_password"))
        mysql_password = file.readlines()[0].strip('\n')
        self.mysql_password = mysql_password
        return mysql_password

    def retrieve_schedule(self):
        """
        Retrieves SF Muni timetable data
        Input: 
            mysql_password        password to MySQL instance 
        Output:
            dictionary with multiple trip_ids
                {trip_id: [departure_time, arrival_time]}
            
        TODO: 
            cache this information when rerunning similar queries 
        Notes:
        mysql select statement's output will look like:
        +----------------+---------------+---------+
        | departure_time | stop_sequence | trip_id |
        +----------------+---------------+---------+
        | 21:28:00       |             1 | 4128319 |
        | 22:06:00       |            45 | 4128319 |
        | 22:28:00       |             1 | 4128320 |
        | 23:05:00       |            45 | 4128320 |
    
        """
        hostname = socket.gethostname()
        query_times = ("select st.departure_time, st.stop_sequence, st.trip_id" 
        " from stop_times st join trips t using (trip_id)" 
        " where t.route_id=%(route_id)s and t.service_id=%(service_id)s"
        " and t.direction_id=%(direction_id)s" 
        " and (st.stop_sequence = %(first_stop)s or st.stop_sequence = %(last_stop)s)" 
        " order by trip_id, stop_sequence asc;") % self.mysql_data
        print query_times
#         % (self.route_id,  self.service_id, self.direction_id, self.first_stop, self.last_stop)
#
#        query_times = "select st.departure_time, st.stop_sequence, st.trip_id" +\
#        " from stop_times st join trips t using (trip_id)" +\
#        " where t.route_id='6011' and t.service_id='1' and t.direction_id='1'" +\
#        " and (st.stop_sequence = '1' or st.stop_sequence = '45')" +\
#        " order by trip_id, stop_sequence asc;" 
        print "running sql query: " + query_times
        self.get_mysql_password()        
        if self.to_log:
            logging.info("Running mysql query: %s", query_times)
        if hostname == self.mysql_local_hostname:
            print "mysql_pass: " + self.mysql_password
            conn = _mysql.connect(user="takingawalk", 
                                  passwd=self.mysql_password,
                                  db="muni")
        else:
            conn = _mysql.connect(host=self.mysql_host,
                                   user="takingawalk", 
                                   passwd=self.mysql_password,
                                   db="muni")
        conn.query(query_times)
        res = conn.store_result()
        rows = res.fetch_row(maxrows=0)
        trips = defaultdict(list)
        for row in rows:
            departure_time = row[0]
            """
            Now due to Muni's awesomeness, we may get hours like 29:05:00
            """ 
            d_hour, d_min, d_sec = departure_time.split(':')
            if int(d_hour) > 23:
                    departure_time = ':'.join([str(int(d_hour)-24),
                                               d_min, 
                                               d_sec])
            trip_id = row[2]
            trips[trip_id].append(departure_time)
        # Now let's organize the dictionary in proper order
#        trips_organized = {}
#        for k, v in trips.iteritems():
#            pass
        if self.to_log:
            print trips
        return trips

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
        conn = connect_to_mongo()
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
            
        return self.start_bus_ids_to_times, self.end_bus_ids_to_times


    """
    Extracts necessary timestamps from data
    Input: a cursor with information of each stop
    {u'route': u'22', u'lon': -122.38929, u'lat': 37.756979999999999,
    u'cur_time': 1299555746.4573929,
    u'_id': ObjectId('4d75a5a2ba528a0d61000164'), u'bus_id': u'8357',
    u'dir': u'null'}
    
    
    Output: dictionary with bus_id -> [t_0, t_1] where
            t_i is the last times a bus has left the start stop
            and the first time a bus has arrived at the end stop
    Discussion:
        When we extract all the data, we want to get preferably the last timestamp
        when a bus has left the starting point, and the first timestamp when the
        bus has arrived. For this reason, since we have sorted our mongo cursors
        with all starting times in descending order, and end times in ascending,
        we will want to only add those timestamps whose difference from previous
        one is greater than the shortest time in which a bus can do a roundtrip. 
        For 22, it is 50 minutes. The logic, is that if adjacent timestamps are
        within that timeframe, it is likely that we have consecutive timestamps
        of a bus hanging out at the end stop. 
    """     
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
        bus_ids_to_times = defaultdict(list)
        for line in mongo_cursor:
            bus_id = line['bus_id']
            timestamp = line['cur_time']
            if timestamp > prev_timestamp + self.LEAST_TIME_TO_DO_ROUNDTRIP:
                self.end_bus_ids_to_times[bus_id].append(timestamp)
                prev_timestamp = timestamp
        return self.end_bus_ids_to_times
    
    def process_data(self, dict_start_time, dict_end_time):
        conn = connect_to_mongo()
        db = conn.muni
        summary = db.summary
        
        tmp_min = []    
        run_times = [] # list of all trip lengths in secs
        late_times = [] # list of latenesses in secs
        trip_ids = []
        bus_ids_not_found = []
        end_times_not_found = []
        num_not_found = 0
        num_deleted = 0
    
        for bus_id in dict_start_time:
            ### TODO:
            ### why only 1 start time ???
            
            tmp_start_times = dict_start_time[bus_id]
            """
            at this point we have a list of epoch times and will need to find closest
                epoch time from schedule
            problems with approach:
                - need to check freshness of data, so maybe the gps was not updated
                 in time
                - how to check if maybe bus left early
            """
            if bus_id in dict_end_time:
                tmp_end_times = dict_end_time[bus_id]
                print "===========" + bus_id + "============"    
                for start_time in tmp_start_times:
                    tm_mon, tm_mday = approx_date(start_time)
                    [schd_epoch_start, schd_epoch_end, trip_id] =\
                     get_schedule_data(start_time, dict_, tm_mon, tm_mday)
                    print "==== %s "  "==== for route id: %s" % \
                    (time.ctime(start_time), trip_id)
                    if trip_id == 0:
                         num_not_found += 1
                         bus_ids_not_found.append({bus_id:start_time})
                    if schd_epoch_start > 0:
                        lateness = start_time-schd_epoch_start
                        if lateness > 0:
                            print "Leaving late by: %s when should have left at %s"\
                            % (str(datetime.timedelta(seconds=int(lateness))),
                             str(time.ctime(schd_epoch_start)))
                        else:
                            print "Leaving early by: %s when should have left at %s"\
                             % (str(datetime.timedelta(seconds=int(abs(lateness)))),
                                 str(time.ctime(schd_epoch_start)))
                    else:
                        print 'Scheduled epoch time is invalid and is %s' % str(schd_epoch_start)
                        print 'Bailing...'
                        break
                    tmp_min = []
                    for end_time in tmp_end_times:
                        diff = end_time - start_time
                        if diff > 0:
                            tmp_min.append(diff)
                    if len(tmp_min) > 0:
                        min_diff = min(tmp_min)
                        #print "==" + time.ctime(end_time) + "=="
                        # 14400 = 3 hours; 9000 = 2.5 hours
                        if 0 < min_diff < 9000:
                            time_shld_take = int(schd_epoch_end-schd_epoch_start)
                            if time_shld_take < 0: #we undershot the estimate by a day
                                time_shld_take += 86400 
                            print "Time taken: %s while should have: %s"\
                             % (str(datetime.timedelta(seconds=min_diff)),
                                 str(datetime.timedelta(seconds=time_shld_take)))
                            print "Arrived at: %s while should have at: %s"\
                             % (str(time.ctime(start_time+min_diff)),
                                 str(time.ctime(schd_epoch_end)))
                            tmp_lateness = min_diff-time_shld_take+lateness
                            output_row = Output(self.route_name,
                                                trip_id,
                                                bus_id,
                                                start_time,
                                                start_time+min_diff,
                                                tmp_lateness,
                                                schd_epoch_start,
                                                schd_epoch_end, 
                                                lateness
                                                )
                            summary.insert(output_row.to_mongo())
                            trip_ids.append(trip_id)
                            late_times.append(tmp_lateness)
    
                            if tmp_lateness < 0: #omfg, muni came early!
                                print "Minutes early: %s"\
                                 % str(datetime.timedelta(seconds=abs(tmp_lateness)))
                            else:
                                print "Minutes late: %s"\
                                % str(datetime.timedelta(seconds=tmp_lateness))
                            run_times.append(min_diff)
                            
                            """
                                After we have already used up the trip_id in the
                                dictionary, then pop it off as we do not want to 
                                use it again.
                                But, we are running over multiple days!
                            """
                    else:
                        print "Finishing time not found with all ending times"\
                        % (tmp_end_times)
                        end_times_not_found.append({bus_id:
                                                    [time.ctime(start_time),
                                                      dict_end_time[bus_id]]
                                                    })
        int_num_left = 0
        for k, v in dict_.iteritems():
            if len(v) == 2:
                int_num_left += 1
            
        avg_run_time = sum(run_times)/len(run_times)
        avg_lateness = sum(late_times)/len(late_times)
        print "average run time: %s based on %s runs"\
         % (str(datetime.timedelta(seconds=avg_run_time)),
             str(len(run_times)))
        if avg_lateness < 0:
            avg_lateness = abs(avg_lateness)
            print "average earliness: %s based on %s runs"\
            % (str(datetime.timedelta(seconds=avg_lateness)),
                str(len(late_times)))
        else:
            print "average lateness: %s based on %s runs"\
             % (str(datetime.timedelta(seconds=avg_lateness)),
                 str(len(late_times)))
        print ("number of dictionary entries left with end times: %d out of total:"
         " %d and number deleted: %d") % (int_num_left, len(dict_), num_deleted)
        print "number of routes not found: %d with pairs: %s"\
         % (num_not_found, str(bus_ids_not_found))
        # Looks like when we do not get to the end we just simply finished a 
        # shortened version of the route and went home to the bus yard after 
        # turning off the GPS
    #    for not_found_item in end_times_not_found:
    #        print "bus_id: %s" % (not_found_item.keys())
    #        print "Starting time: %s" % (not_found_item.values()[0][0])
    #        for bus_time in not_found_item.values()[0][1]:
    #            print "Ending time: %s" % (time.ctime(bus_time))
        
        dups = []
        trip_ids.sort()
    
        for trip_id in trip_ids:
            if trip_ids.count(trip_id) > 1:
                dups.append(trip_id)
        print "duplicate route ids: %s" % (str(list(set(dups))))

# Helpers

def approx_date(epoch_secs):
    tme = time.localtime(epoch_secs)    
    return tme.tm_mon, tme.tm_mday

def convert_to_epoch_time(a_time, tm_year, tm_mon, tm_mday):
    new_datetime = datetime.datetime(*time.strptime(a_time, '%H:%M:%S')[0:5])
    tuple = new_datetime.replace(year=tm_year, 
                                 month=tm_mon, 
                                 day=tm_mday).timetuple()
    epoch = time.mktime(tuple)
    return epoch
        
# Doers

def get_schedule_data(cand_secs, dict_, tm_mon, tm_mday):
    """
    Provides best approximation of closest departing and arrival times given
    an epoch time
    Input: 
        cand_secs        epoch time (secs)
        dict_            dictionary containing schedules for a route
        tm_mon            month which we are analyzing
        tm_mday            day in the month which we are analyzing
    
    Output:
        list containing best guess at starting and ending time with a route_id 
        
    Issues:
        How to approximate the time the bus should have left
        Assume that all buses leave late, so find the first scheduled
            leave time after the candidate and return the previous
        TODO:
            figure out a way to speed up the process, like 
            pre-build dictionaries
    """
    epoch_times_start = []
    epoch_times_end = []
    dict_times = {}
#    for x in start_rows:
    for k, v in dict_.items():
            if len(v) > 1:
                dep_time = v[0]
                end_time = v[1]
                tmp_start = convert_to_epoch_time(dep_time, 2011, tm_mon,
                                                   tm_mday)
                tmp_end = convert_to_epoch_time(end_time, 2011, tm_mon,
                                                 tm_mday)
                dict_times[tmp_start] = [tmp_end, k]
                epoch_times_start.append(tmp_start)
                epoch_times_end.append(tmp_end)
            else:
                # No end time
                pass
    # want uniqueness
    tmp_list = epoch_times_start
    epoch_times_start = list(set(tmp_list))        
    epoch_times_start.sort()
    tmp_list = epoch_times_end
    epoch_times_end = list(set(tmp_list))             
    epoch_times_end.sort()
    
    start = 0
    end = 0
    trip_id = 0
    for i, tmp_time in enumerate(epoch_times_start):
        # if the run started within 32 secs of next scheduled run
        # helping decrease error b/c we are collecting data every 30 secs
        """
        Allowing Muni to leave up to 200 secs early (gasp!)
        """
        if cand_secs+200 > tmp_time > cand_secs-62:
            start = epoch_times_start[i]
            end, trip_id = dict_times[start]
            break
#    TODO: fix this
        elif tmp_time > cand_secs:
            start = epoch_times_start[i-1]
            end, trip_id = dict_times[start]
            break
    return [start, end, trip_id]

    
    
    
def connect_to_mongo():
    mongodb_local_hostname = 'ip-10-170-26-73'
    mongodb_host = 'ec2-50-18-72-59.us-west-1.compute.amazonaws.com'
    hostname = socket.gethostname()
    if hostname == mongodb_local_hostname:
        conn = pymongo.Connection()
    else:
        conn = pymongo.Connection(mongodb_host)
        
    return conn



if __name__ == "__main__":
        LOG_FILENAME = '/tmp/Muni.out'
        logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO)
        os.environ['TZ'] = 'US/Pacific'
        time.tzset()
        start = time.time()
        
        try:
            opts, args = getopt.getopt(sys.argv[1:], "d:hlp:r:t:v",
                                        ["direction=","help", "route="])
        except getopt.GetoptError, err:
            # print help information and exit:
            print "Error: " + str(err) # will print something like "option -a not recognized"
            sys.exit(2)
        direction = "N"
        route_name = None
        verbose = False
        for o, a in opts:
            if o == "-v":
                verbose = True
            # Direction will be used in the future
            elif o in ("-d", "--direction"):
                direction = a
            elif o in ("-h", "--help"):
    #            usage()
                sys.exit()
            elif o in ("-r", "--route"):
                route_name = a
            else:
                assert False, "unhandled option"

        analyze = Analyze(route_name)
        analyze.setup_route_info()
        dict_ = analyze.retrieve_schedule()
        print "====finished retrieving mysql schedule"
        print "==== starting to retrieve data ===="
        dict_start, dict_end = analyze.get_data()
        print "==== starting processing data ===="
        analyze.process_data(dict_start, dict_end)
        taken = time.time()-start
        print "time taken to run: %s"\
         % (str(datetime.timedelta(seconds=taken)))
        print "===DONE===" * 20