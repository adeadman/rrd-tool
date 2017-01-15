# -*- coding: utf-8 -*-
import sys
import os
import unittest
import datetime

# Path hack lets us import sibling packages
sys.path.insert(0, os.path.abspath('..'))
import round_robin as rr

TEST_DB = 'test.db'

class DatabaseSetupTests(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        os.remove(TEST_DB)

    def test_database_creation(self):
        #print("Testing Database Creation")
        rrd = rr.open_database(('SQLite', TEST_DB))

        # not a very good test that the db has been properly inited
        self.assertTrue(os.path.isfile(TEST_DB))

class HelperFunctionsTest(unittest.TestCase):
    def test_helper_functions(self):
        # Use an arbitrary datetime - 10 Jan 2017 14:42:42
        atime = datetime.datetime(2017,1,10,14,42,42)
        atimestamp = 1484026962

        #print("Testing round_robin.time_to_timestamp()")
        self.assertEqual(atimestamp, rr.time_to_timestamp(atime),
                "time_to_timestamp() function failing.")
        #print("Testing round_robin.timestamp_to_time()")
        self.assertEqual(atime, rr.timestamp_to_time(atimestamp),
                "timestamp_to_time() function failing.")

        #print("Testing round_robin.timestamp_minute()")
        self.assertEqual(rr.time_to_timestamp(datetime.datetime(2017,1,10,14,42,0)),
                         rr.timestamp_minute(atimestamp),
                         "timestamp_minute() function failing.")

        #print("Testing round_robin.timestamp_hour()")
        self.assertEqual(rr.time_to_timestamp(datetime.datetime(2017,1,10,14,0,0)),
                         rr.timestamp_hour(atimestamp),
                         "timestamp_hour() function failing.")

class RoundRobinDbSaveTests(unittest.TestCase):
    good_data = [( 60, 25.0),
                 (120, 30.0),
                 (180, 35.0),
                 (240, 40.0),
                 (300, 45.0),
                 (360, 50.0)]

    def setUp(self):
        self.rrd = rr.open_database(('SQLite', TEST_DB))

    def tearDown(self):
        os.remove(TEST_DB)
        #pass

    def test_save(self):
        
        
        for ts, val in self.good_data:
            self.rrd.save(ts, val)

        self.assertEqual(360, self.rrd.last_timestamp, 
                         "Minutes table is not being updated on save.")
        self.assertEqual(0, self.rrd.last_hour_timestamp, 
                         "Hours table is not being updated on save.")
        self.assertEqual(self.rrd.get_timestamp_value('Hours', 
                                     self.rrd.last_hour_timestamp),
                        25, "Hour table is not being updated with minimum value.")

        # Test that saving a value more than 2 minutes after the last save will
        # create an interim None value in Minutes table
        self.rrd.save(self.good_data[-1][0] + 120, 60)
        mins = self.rrd.query('minutes')
        self.assertEqual(mins[-2][0], self.good_data[-1][0] + 60, 
                         "Interim timestamp not created on save.")
        self.assertIsNone(mins[-2][1], "Interim value not None.")

        # Test that saving a timestamp previous to the latest one  fails
        self.assertRaises(ValueError, rr.RoundRobinDb.save, 
                                        self.rrd, *self.good_data[-1]) 

        # Test that saving a value more than 2 hours after the last save will
        # create an interim None value in Minutes table
        self.rrd.save(7220, 100)
        hours = self.rrd.query('hours')
        self.assertEqual(hours[-2][0], 3600,
                         "Interim hour timestamp not created on save.")
        self.assertIsNone(hours[-2][1], "Interim hour value not None.")


class RoundRobinDbUQueryTests(unittest.TestCase):
    good_data = [(min*60, min*5 + 20.0) for min in range(0,62)]

    def setUp(self):
        self.rrd = rr.open_database(('SQLite', TEST_DB))
        # Save our test data in the database
        for ts, val in self.good_data:
            self.rrd.save(ts, val)

    def tearDown(self):
        os.remove(TEST_DB)

    def testMinutesQuery(self):
        # Testing minutes property
        mins = self.rrd.minutes
        
        # Test that our 62 entries have not overflown the RRD
        # and that the final entry in our minutes property is
        # the final entry of good data.
        self.assertEqual(60, len(mins))
        self.assertEqual(self.good_data[-1], mins[-1])

    def testHoursQuery(self):
        # Test query method with `hours` argument
        hours = self.rrd.query('hours')

        # Test that our 62 entries have properly moved the second
        # hour into the RRD 
        self.assertIsNone(hours[-3][0]) # guard value
        self.assertIsNotNone(hours[-2][0])
        self.assertIsNotNone(hours[-2][1])
        self.assertIsNotNone(hours[-1][0])
        self.assertIsNotNone(hours[-1][1])
        # and that our first data entry's value
        # has kept the minimum value from good_data[0]
        self.assertEqual(self.good_data[0], hours[-2])
