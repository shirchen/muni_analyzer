'''
Created on Apr 16, 2010

@author: dima


Determines the closest 2 stops for a route. Will do so by breaking up the route
    into segments
    
    * loadStopsDict

'''
from Muni.route import Route
import math

class Location(object):
    '''
    classdocs
    '''
    pass


    def __init__(self, str_route):
        '''
        Constructor
        '''
        self.str_route = str_route
    
    '''
        Idea is to find the closest stop based on longitude 
        and latitude coordinates for a bus. For now, we are 
        not worried whether the stop is ahead of behind. Just
        closest for now. 
        
        We achieve that by getting langitude/latitude 
        coordinates for all stops, and then finding the one
        that is the shortest distance from our two coordinates.
    
    '''    
    def findClosestStop(self, float_lat, float_lon):
        
        ''' setting up dicts '''
        routeObj = Route(self.str_route)
#        dict_titleToStop = routeObj.loadTitleToStopDict()
#        dict_idToTitle = dict((v,k) for k,v in dict_titleToStop.iteritems())
        dict_stopIDtoCoord = routeObj.loadStopsDict()
        
        tmp_list_closest_stop = []
        # Need a really big number
        float_shortest_distance = 1
        # Now search through all values of the dictionary
        for tmp_list in dict_stopIDtoCoord.values():
            tmp_float_lon = float(tmp_list[0])
            tmp_float_lat = float(tmp_list[1])
            
            float_dist = self.dist(float_lon, float_lat, tmp_float_lon, 
                                   tmp_float_lat)
            if float_dist < float_shortest_distance:
                tmp_list_closest_stop = tmp_list
            
        
        return tmp_list_closest_stop
            
        
    def dist(self, float_a, float_b, float_my_a, float_my_b):
        float_diff_a = float_my_a-float_a
        float_diff_b = float_my_b-float_b
        return math.sqrt(math.pow(float_diff_a,2)+
                         math.pow(float_diff_b,2))