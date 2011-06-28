import datetime
import os
import pymongo
import socket
import time
import _mysql


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
                d_hour, d_min, d_sec = dep_time.split(':')
                e_hour, e_min, e_sec = end_time.split(':')
#    Now due to Muni's awesomeness, we may get hours like 29:05:00
#    TODO: move this out when creating dict_
                if int(d_hour) > 23:
                    dep_time = ':'.join([str(int(d_hour)-24), d_min, d_sec])
                if int(e_hour) > 23:
                    end_time = ':'.join([str(int(e_hour)-24), e_min, e_sec])
                tmp_start = convert_to_epoch_time(dep_time, 2011, tm_mon, tm_mday)
                tmp_end = convert_to_epoch_time(end_time, 2011, tm_mon, tm_mday)
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
    route_id = 0
    for i, tmp_time in enumerate(epoch_times_start):
        # if the run started within 32 secs of next scheduled run
        # helping decrease error b/c we are collecting data every 30 secs
        if cand_secs+62 > tmp_time > cand_secs-62:
            start = epoch_times_start[i]
            end, route_id = dict_times[start]
            break
#    TODO: fix this
        elif tmp_time > cand_secs:
            start = epoch_times_start[i-1]
            end, route_id = dict_times[start]
            break
    return [start,end, route_id]
    
"""
    Retrieving schedule for the 22. Route_id is hard coded as well as beginning and end stops.
    TODO: cache this information when rerunning similar queries 
"""

def retrieve_schedule():
    hostname = socket.gethostname()
    
    query_times = """select st.departure_time, st.stop_sequence, st.trip_id from stop_times st join trips t using (trip_id) where t.route_id='6011' and t.service_id='1' and t.direction_id='1' and (st.stop_sequence = '1' or st.stop_sequence = '45') order by trip_id asc;"""
    
    if hostname == 'domU-12-31-39-09-C5-9A':
        conn = _mysql.connect(user="takingawalk", passwd="sheandhim", db="muni")
    else:
        conn = _mysql.connect(host="ec2-50-16-77-90.compute-1.amazonaws.com", user="takingawalk", passwd="sheandhim", db="muni")
        
    conn.query(query_times)
    res = conn.store_result()
    rows = res.fetch_row(maxrows=0)

    dict_ = {}
    
    for x in rows:
        if x[2] not in dict_:
            dict_[x[2]] = []
            dict_[x[2]].append(x[0])
        else:
            dict_[x[2]].append(x[0])

    return dict_

    
def get_data(dict_):
    epoch_from = 1301641200
    epoch_to = epoch_from+60*60*24
    # letting runs finish for 2 more hours
    # ideally, want to make this a function of time from schedule plus some variation, like 1 hour just in case 
    epoch_to_adjusted = epoch_to + 7200
    hostname = socket.gethostname()
    if hostname == 'domU-12-31-39-09-C5-9A':
        conn = pymongo.Connection()
    else:
        conn = pymongo.Connection('ec2-50-16-77-90.compute-1.amazonaws.com')
    db = conn.muni_database
    
    print "==== Collecting starting runs from %s to %s ===="\
     % (str(time.ctime(epoch_from)), str(time.ctime(epoch_to)))
    tmp_list_i = []
#    for x in db.location.find({ "lat": {"$gte" :37.7604 }, "lat": {"$lte": 37.7606},  "lon":{"$lte":-122.38},"lon":{"$gte":-122.39}, "cur_time": {"$gte": 1299526830}, "bus_id": "5444"}):
#    for x in db.location.find({ "$or": [ {"dir": "22_IB2"}, {"$dir" : "22_OB2"} ], "lat": {"$lte": 37.7606}, "lat": {"$gte" :37.7604 }, "lon":{"$lte":-122.38},"lon":{"$gte":-122.39}}):
    for x in db.location.find({"lat": {"$gte" :37.7604, "$lte": 37.7606},
                               "lon":{"$lte":-122.38, "$gte":-122.39},
                               "cur_time": {"$gte": epoch_from, 
                                            "$lte":epoch_to}}):
        tmp_list_i.append(x)
    
    tmp_list = []
#for x in db.location.find({ "$or": [ {"dir": "22_IB2"}, {"$dir" : "22_OB2"} ], "lat": {"$lte": 37.8025}, "lat": {"$gte" :37.801 }, "lon":{"$lte":-122.4362},"lon":{"$gte":-122.4368 }}):
# cur_time condition for captured data after monday
    print "=== Collecting end runs from %s to %s ====" % (str(time.ctime(epoch_from)), str(time.ctime(epoch_to_adjusted)))
#    for x in db.location.find({"lat": {"$lte": 37.8025}, "lat": {"$gte" :37.801 },  "lon":{"$lte":-122.4362},"lon":{"$gte":-122.4368 }, "cur_time": {"$gte": 1299526830}}):
#    for x in db.location.find({"lat": {"$lte": 37.8025}, "lat": {"$gte" :37.801 },  "lon":{"$lte":-122.4362},"lon":{"$gte":-122.4368 }, "cur_time": {"$gte": 1299526830}, "bus_id": "5444"}):
    for x in db.location.find({"lat": {"$lte": 37.8025, "$gte" :37.801 },
                                "lon":{"$lte":-122.4362, "$gte":-122.4368 },
                                "cur_time": {"$gte": epoch_from,
                                              "$lte":epoch_to_adjusted}}):
        tmp_list.append(x)
        
    return tmp_list_i, tmp_list


"""
Input: a list with information of each stop
{u'route': u'22', u'lon': -122.38929, u'lat': 37.756979999999999, u'cur_time': 1299555746.4573929, u'_id': ObjectId('4d75a5a2ba528a0d61000164'), u'bus_id': u'8357', u'dir': u'null'}


Output: dictionary with bus_id -> [t_0, t_1] where
        t_i is the last times a bus has left the start stop
        and the first time a bus has arrived at the end stop
"""           
def massage_data(tmp_list, bool_start=True):
    dict_ids_to_times = {}
    for i, line in enumerate(tmp_list):
        bool_new = True
        cur_bus_id = line['bus_id']
        cur_time = line['cur_time']
        if cur_bus_id in dict_ids_to_times:
            tmp_list_times = dict_ids_to_times[cur_bus_id]
            # if starting times want to either:
            #    a. this cur_time is within 62 secs of previous
            #        then replace the previous with cur_time, as a bus
            #        could just be chilling at the starting stop
            #   b. if its more than 50 minutes after every time in the list
            #       then append to the list
            for tmp_time in tmp_list_times:
                # if end stop, then just want to grab the first time we got there
                if bool_start and cur_time < tmp_time + 62:
                    tmp_list_times.remove(tmp_time)
                    break
                elif cur_time < tmp_time + 3000:
                    bool_new = False
                    break
            if bool_new:
                tmp_list_times.append(cur_time)
                dict_ids_to_times[cur_bus_id] = tmp_list_times
        else:
            dict_ids_to_times[cur_bus_id] = [cur_time]
    return dict_ids_to_times

def process_data(dict_start_time, dict_end_time):
    tmp_min = []    
    run_times = [] # list of all trip lengths in secs
    late_times = [] # list of latenesses in secs
    num_not_found = 0
    num_deleted = 0

    for bus_id in dict_start_time:
        tmp_start_times = dict_start_time[bus_id]
        """
        at this point we have a list of epoch times and will need to find closest
            epoch time from schedule
        problems with approach:
            - need to check freshness of data, so maybe the gps was not updated in time
            - how to check if maybe bus left early
        """
        if bus_id in dict_end_time:
            tmp_end_times = dict_end_time[bus_id]
            print "===========" + bus_id + "============"    
            for start_time in tmp_start_times:
                tm_mon, tm_mday = approx_date(start_time)
                [schd_epoch_start, schd_epoch_end, route_id] =\
                 get_schedule_data(start_time, dict_, tm_mon, tm_mday)
                print "====" + str(time.ctime(start_time)) + "==== for route id: " + str(route_id)
                print "==== %d "  "==== for route id: %d" % \
                (time.ctime(start_time), route_id)
                if schd_epoch_start > 0:
                    lateness = start_time-schd_epoch_start
                    if lateness > 0:
                        print "Leaving late by: %s " 
                        "when should have left at %s"\
                        % (str(datetime.timedelta(seconds=int(lateness))),
                         str(time.ctime(schd_epoch_start)))
                    else:
                        print "Leaving early by:" + str(datetime.timedelta(seconds=int(abs(lateness)))) + " when should have left at " + str(time.ctime(schd_epoch_start))
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
                        print "Time taken:" + str(datetime.timedelta(seconds=min_diff)) + " while should have: " + str(datetime.timedelta(seconds=time_shld_take))
                        tmp_lateness = min_diff-time_shld_take
                        if tmp_lateness < 0: #omfg, muni came early!
                            tmp_lateness = abs(tmp_lateness)
                            print "Minutes early: " + str(datetime.timedelta(seconds=tmp_lateness))
                        else:
                            print "Minutes late: " + str(datetime.timedelta(seconds=tmp_lateness))
                        run_times.append(min_diff)
                        late_times.append(tmp_lateness)
                        
                        if route_id == 0:
                            num_not_found += 1
                        """
                            After we have already used up the route_id in the dictionary, then 
                            pop it off as we do not want to use it again.
                            But, we are running over multiple days!
                        """
                        if route_id in dict_:
                            del dict_[route_id]
                            num_deleted += 1

    int_num_left = 0
    for k, v in dict_.iteritems():
        if len(v) == 2:
            int_num_left += 1
        
    avg_run_time = sum(run_times)/len(run_times)
    avg_lateness = sum(late_times)/len(late_times)
    print "average run time: %s based on %s runs"\
     % (str(datetime.timedelta(seconds=avg_run_time)), str(len(run_times)))
    print "average lateness: %s based on %s runs"\
     % (str(datetime.timedelta(seconds=avg_lateness)), str(len(late_times)))
    print "number of dictionary entries left with end times: %d out of total: %d and number deleted: %d" % (int_num_left, len(dict_), num_deleted)
    print "number of routes not found: %d" % (num_not_found)

if __name__ == "__main__":
        os.environ['TZ'] = 'US/Pacific'
        time.tzset()
        start = time.time()
        dict_ = retrieve_schedule()
        print "====finished retrieving mysql schedule"
    #get_schedule_data()    
        list_start, list_end = get_data(dict_)
        print "==== starting to massage data ====" 
        dict_start = massage_data(list_start, True)
        dict_end = massage_data(list_end, False)
        print "==== starting processing data ===="
        process_data(dict_start, dict_end)
        taken = time.time()-start
        print "time taken to run: %s" % (str(datetime.timedelta(seconds=taken)))
        print "=======" * 10