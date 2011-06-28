'''
Created on Oct 25, 2010

@author: dima
'''

class Analyzer(object):
    pass


'''
    Purpose of this class is to figure out just how late a Muni bus is
    relative to its predicted time of arrival.
    
    We achieve this by 
    
    sample prediction arrival data:
    Current time:   StopID  Seconds: Minutes: epochTime: isDeaprture:       dirTag: Vehicle: Block
    1288049385.19   3342    13      0       1288049397754      false        22_OB2  5470     220
     
    sample location data:
    Current time    Id:     Lat:             Lon:      Leading ID:  Dir Tag Out of date (secs):
    1288048422.56   5411    37.76588        -122.40507              22_OB2  1
   
    So, what we do is for each vehicle ID, we gather all prediction data along the entire
    route and then 
    
    
'''