from typing import List
import numpy as np
import pandas as pd
import simpy
from simpy.core import Environment
from simpy.resources.store import FilterStore
from src.utils import sample_point_in_geometry, sample_random_trip_time, cdate

class Rider(object):
    def __init__(self, num: int, trip_endpoint_data: pd.DataFrame, geo_df: pd.DataFrame, env: Environment,
                 request_store: FilterStore, request_collection: List, verbose: bool=True):
        self.num = num
        self.trip_endpoint_data = trip_endpoint_data
        self.geo_df = geo_df
        self.env = env
        self.request_store = request_store
        self.verbose = verbose
        
        # Variables to keep track off
        self.pos = None
        self.pos_point = None
        self.des = None
        self.des_point = None
        
        # Trip status
        self.matched_with_driver = False
        self.__available = True
        self.completed = False
        
        # Timing
        self.start_wait_time = None
        self.wait_time = 0
        self.driver_wait_time = 0
        self.ride_time = 0
        
        # Determine patience (NONE for infinity)
        self.match_patience = 5
        self.wait_patience = None
        
        # Initialize location
        self.initialize_location()
        
        # Save request for analysis
        request_collection.append(self)
        
        # Start the request process when instance is created
        self.action = env.process(self.request())
        
    @property
    def available(self):
        return self.__available and \
               self.matched_with_driver == False and \
               self.completed == False
        
    @property
    def cancelled(self):
        return self.__available == False
        
    @property
    def total_trip_time(self):
        return self.wait_time + self.driver_wait_time + self.ride_time
        
    def initialize_location(self):
        """
        Initializes current location and destination and samples random location within TAZ for visualization.
        Assumes rider rather walks if time driving is less than one minute.
        """
        # Determine hour of day and weekday
        hour = self.env.now / 60
        hour_of_day = int(hour % 24)
        weekday = int((self.env.now / 60 / 24) % 7)
        
        # TODO: RANDOM CHOICE OUT OF TAZs WHICH ARE A CERTAIN MINIMUM L1 DISTANCE AWAY
        # TODO: Given location, sample from TAZ s.t. average uber drive is 5.2 (or whatever) miles llong
        
        # Sample position
        probs = self.trip_endpoint_data.loc[(weekday, hour_of_day)]['pickups']
        self.pos = np.random.choice(self.trip_endpoint_data.loc[(weekday, hour_of_day)]['MOVEMENT_ID_uber'], size=1, p=probs)[0]
        self.pos_point = sample_point_in_geometry(self.geo_df.loc[self.pos]['geometry'], 1)
        
        # Sample destination - if < 1 minute, rather walk
        probs = self.trip_endpoint_data.loc[(weekday, hour_of_day)]['dropoffs']
        while self.des is None or sample_random_trip_time(hour_of_day, self.pos, self.des) < 1.:
            self.des = np.random.choice(self.trip_endpoint_data.loc[(weekday, hour_of_day)]['MOVEMENT_ID_uber'], size=1, p=probs)[0]
            self.des_point = sample_point_in_geometry(self.geo_df.loc[self.des]['geometry'], 1)
        
        
    def request(self):
        """
        Rider pool requests a ride, given their randomly sampled position and destination.
        
        The following variables are matched with real-world ridesharing data:
            - Day of week
            - Hour of Day
            - Geospatial pickup TAZ distribution
            - Geospatial dropoff TAZ destination
        """
        # Wait for pickup
        self.request_store.put((self.env.now, self))
        if self.verbose:
            print(f'{cdate(self.env.now)}: Rider {self.num:5.0f} requests: TAZ {self.pos} -> TAZ {self.des}')
        
        try:
            yield self.env.process(self.wait_for_match())
        except simpy.Interrupt:
            pass
        
        end_time = self.env.now
        self.wait_time = end_time - self.start_wait_time
        if self.wait_time >= self.match_patience:
            self.__available = False
            if self.verbose:
                print(f'{cdate(self.env.now)}: Rider {self.num:5.0f} waited too long for match -> cancelled')
            return
        
        self.matched_with_driver = True
        
        # Got matched with driver, waiting for driver arrival
        if self.verbose:
            print(f'{cdate(self.env.now)}: Rider {self.num:5.0f} got matched, waited for {self.wait_time:2.2f} @ TAZ {self.pos}')
        yield self.env.process(self.wait_for_pickup())
        
        # Complete trip
        if self.verbose:
            print(f'{cdate(self.env.now)}: Rider {self.num:5.0f} starts trip, waited for {self.driver_wait_time:2.2f} @ TAZ {self.pos}')
        yield self.env.timeout(self.ride_time)
        self.completed = True
        
    def set_next_trip_duration(self, wait_time, ride_time):
        """
        Set times for waiting for driver and coming trip
        """
        self.driver_wait_time = wait_time
        self.ride_time = ride_time
        
    def wait_for_match(self):
        """
        Implements a wait process for driver match, optionally with a patience.
        """
        self.start_wait_time = self.env.now
        if self.match_patience is None:
            yield self.env.timeout(simpy.core.Infinity)
        else:
            yield self.env.timeout(self.match_patience)
            
    def wait_for_pickup(self):
        """
        Implements a wait process for driver coming to rider, optionally with a patience.
        """
        if self.wait_patience is not None and self.driver_wait_time > self.wait_patience:
            yield self.env.timeout(0.5) # wait 30 seconds and then decide to cancel
            self.__available = False
            if self.verbose:
                print(f'{cdate(self.env.now)}: Rider {self.num:5.0f} thinks wait time is too long -> cancelled')
            return
            # TODO: TELL DRIVER TO GET NEW REQUEST
        
        yield self.env.timeout(self.driver_wait_time)