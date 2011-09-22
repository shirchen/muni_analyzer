'''
Created on Sep 21, 2011

@author: shirchen

This class is responsible for pulling schedule
'''
import _mysql
import datetime
import logging
import os
import socket
import time
from collections import defaultdict


class Schedule_Pull(object):
    
    def __init__(self, route_name):
        self.mysql_host = "ec2-50-18-72-59.us-west-1.compute.amazonaws.com"
        self.mysql_local_hostname = 'ip-10-170-26-73'
        self.route_name = route_name
        self.to_log = True
        self.trips = defaultdict(list)
    
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
        if self.to_log:
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
            self.trips[trip_id].append(departure_time)
        # Now let's organize the dictionary in proper order
#        trips_organized = {}
#        for k, v in trips.iteritems():
#            pass
        if self.to_log:
            print self.trips
#        return self.trips
    
    def get_schedule_data(self, cand_secs):
        """
        Provides best approximation of closest departing and arrival times given
        an epoch time
        Input: 
            cand_secs        epoch time (secs)
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
        tm_mon, tm_mday = self.approx_date(cand_secs)
    #    for x in start_rows:
        for k, v in self.trips.items():
                if len(v) > 1:
                    dep_time = v[0]
                    end_time = v[1]
                    tmp_start = self.convert_to_epoch_time(dep_time, 2011, tm_mon,
                                                       tm_mday)
                    tmp_end = self.convert_to_epoch_time(end_time, 2011, tm_mon,
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
        if self.to_log:
            print start, end, trip_id
        return [start, end, trip_id]

    def approx_date(self, epoch_secs):
        tme = time.localtime(epoch_secs)    
        return tme.tm_mon, tme.tm_mday
    
    def convert_to_epoch_time(self, a_time, tm_year, tm_mon, tm_mday):
        new_datetime = datetime.datetime(*time.strptime(a_time, '%H:%M:%S')[0:5])
        tuple = new_datetime.replace(year=tm_year, 
                                     month=tm_mon, 
                                     day=tm_mday).timetuple()
        epoch = time.mktime(tuple)
        return epoch

def tests():
    pass


if __name__ == "__main__":
    sched_pull = Schedule_Pull('22')
    sched_pull.setup_route_info()
    sched_pull.retrieve_schedule()
    print sched_pull.get_schedule_data(1316659549)
    