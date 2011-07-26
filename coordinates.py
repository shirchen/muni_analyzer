'''
Created on Mar 4, 2011

@author: shirchen
'''

#from Muni.downloader import Downloader

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