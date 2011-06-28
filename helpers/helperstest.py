'''
Created on Feb 28, 2011

@author: shirchen

Unit tests to test conversion of stop_times.txt file
'''
import convertSchedule
import unittest
import datetime
import time


class ConvertSchedule(unittest.TestCase):
    hours = ((9,'4128896,09:10:00,09:00:00,3410,1, , , , '),
             (12,'4128896,12:10:00,09:00:00,3410,1, , , , '),
             (03,'4128896,03:10:00,09:00:00,3410,1, , , , '),
             )

    mins = ((10,'4128896,09:10:00,09:00:00,3410,1, , , , '),
             (06,'4128896,09:06:00,09:00:00,3410,1, , , , '),
             (12,'4128896,09:12:00,09:00:00,3410,1, , , , '), 
             )
    
    stamps = ((1298970600, '4128896,09:10:00,09:00:00,3410,1, , , , '),
              (1298988720, '4128896,14:12:00,09:00:00,3410,1, , , , '),
              (1298998740, '4128896,16:59:00,09:00:00,3410,1, , , , '),
              )

    cs = convertSchedule.ConvertSchedule()

    """
    Test convertSchedule.ConvertSchedule().parseStopLine
    """
    def testTimeParsing(self):
        for hour, stamp in self.hours:
            hr, min = self.cs.parseStopsLine(stamp)
            self.assertEqual(hour, hr)
            
        for min, stamp in self.mins:
            hr, mn = self.cs.parseStopsLine(stamp)
            self.assertEqual(min, mn)
            
    """
    Test convertSchedule.ConvertSchedule().parseStopLine
    """
    def testConvertToEpoch(self):
        for epoch, stamp in self.stamps:
            # problem: how to compare today's and previous timestamps
            # sol?: rig the date, month and year to be the same
            # and compare just the rest
            int_ = self.cs.convertToEpoch(stamp,2011,03,01)
            self.assertEqual(epoch, int_)
            

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()