from typing import List
import numpy as np
import pandas as pd
import simpy
from simpy.core import Environment
from simpy.resources.store import FilterStore
from src.utils import cdate
from src.simulation.params import INITIAL_TIME
from src.utils.sampling import sample_point_in_geometry

class Driver(object):
    def __init__(self, num: int, trip_endpoint_data: pd.DataFrame, geo_df: pd.DataFrame, num_driver_df: pd.DataFrame, env: Environment,
                 driver_store: FilterStore, driver_collection: List, num_active_drivers: List, verbose: bool=True):
        """Instantiates a driver element for the simulation.

        Args:
            num (int): unique driver number
            trip_endpoint_data (pd.DataFrame): dataframe to sample start location from
            geo_df (pd.DataFrame): dataframe with geographic geometries to sample point within TAZ for visualization
            num_driver_df (pd.DataFrame): dataframe containing supply side data for uber drivers
            env (Environment): simpy environment
            driver_store (FilterStore): container of all available drivers
            driver_collection (List): list of all drivers
            num_active_drivers (List): list containing one number which is the current number of active drivers
            verbose (bool, optional): verbose setting. Defaults to True.
        """
        self.num = num
        self.env = env
        self.driver_store = driver_store
        self.num_driver_df = num_driver_df
        self.num_active_drivers = num_active_drivers
        self.verbose = verbose
        
        # Determine hour of day and weekday
        hour = env.now / 60
        hour_of_day = int(hour % 24)
        weekday = int((env.now / 60 / 24) % 7)
        
        # Sample starting position
        probs = trip_endpoint_data.loc[(weekday, hour_of_day)]['dropoffs']
        self.start_pos = np.random.choice(trip_endpoint_data.loc[(weekday, hour_of_day)]['MOVEMENT_ID_uber'], size=1, p=probs)[0]
        self.curr_pos = self.start_pos
        
        # Variables to keep track off        
        self.oos_wait = 0
        self.oos_drive = 0
        self.trip_total = 0
        self.__available = True
        self.ontrip = False
        
        # Patience (NONE for infinity)
        self.patience = None
        
        # Trips
        self.num_trips = 0

        # Last known location
        self.last_coming_from = sample_point_in_geometry(geo_df.loc[self.start_pos]['geometry'], 1)
        self.last_heading_to = self.last_coming_from # sample_point_in_geometry(geo_df.loc[self.start_pos]['geometry'], 1)
        self.is_oos = True
        
        # Next Location (set by Trip)
        self.rider_loc = None
        self.dest_loc = None
        
        # Save driver for analysis
        driver_collection.append(self)
        
        # start the drive process when instance is created
        self.action = env.process(self.drive())
        
    @property
    def available(self):
        return self.__available and self.ontrip == False
        
    @property
    def offline(self):
        return self.__available == False
        
    @property
    def oos_total(self):
        return self.oos_wait + self.oos_drive
    
    @property
    def total_time_active(self):
        return self.oos_wait + self.oos_drive + self.trip_total

    def drive(self):
        """
        Driver starts servicing requests.
        """
        # Go online on uber app
        self.go_online()
        if self.env.now > INITIAL_TIME and self.verbose:
            print(f'{cdate(self.env.now)}: Driver {self.num:5.0f} dispatched @ TAZ {self.start_pos}. Active drivers: {self.num_active_drivers[0]:,}')
        
        while True:
            
            # Signal not on trip with rider right now
            self.is_oos = True
            
            # Signal availability
            self.driver_store.put((self.env.now, self))
            
            # Wait for request
            try:
                yield self.env.process(self.wait_for_request())
            except simpy.Interrupt:
                pass
            
            if self.offline:
                return
            
            # Received request
            self.ontrip = True
            if self.verbose:
                print(f'{cdate(self.env.now)}: Driver {self.num:5.0f} got request, waited for {self.oos_wait:2.2f} @ TAZ {self.curr_pos}')
            
            # Drive to rider
            yield self.env.process(self.oos_drive_to_rider())
            
            # Complete trip
            self.is_oos = False
            yield self.env.process(self.complete_trip())

            # Decide if should head home
            if self.should_head_home():
                self.go_offline()
                if self.verbose:
                    print(f'{cdate(self.env.now)}: Driver {self.num:5.0f} is heading home. Going offline. Active drivers: {self.num_active_drivers[0]:,}')

                return


    def go_offline(self):
        """Sets driver to offline.
        """
        self.num_active_drivers[0] -= 1
        self.__available = False

    
    def go_online(self):
        """Sets driver to online.
        """
        self.num_active_drivers[0] += 1
        self.__available = True

    
    def should_head_home(self):
        """Process for driver to decide if heading home

        Note: the process depends on how close the current number of online drivers
              matches the overall average number of online drivers at that time.
        """

        hour = (self.env.now / 60)
        hour_of_day = int(hour % 24)
        minute = int(self.env.now % 60)
        target_uber_supply = self.num_driver_df.loc[(hour_of_day, minute), 'n_drivers']
        num_active = self.num_active_drivers[0]
        return (num_active - target_uber_supply) > 1


    def wait_for_request(self):
        """
        Wait until a new request is received.
        """
        # Wait until needed
        start_time = self.env.now
        if self.patience is None:
            yield self.env.timeout(simpy.core.Infinity)
        else:
            yield self.env.timeout(self.patience)

        # Calculate the time spent waiting
        end_time = self.env.now
        wait_time = end_time - start_time
        self.oos_wait += wait_time

        # Go offline if wait was too long
        #if wait_time == self.patience:
        #    self.__available = False
        #    if self.verbose:
        #        print(f'{cdate(self.env.now)}: Driver {self.num:5.0f} waited too long -> offline')
            
            
    def set_next_destination(self, rider_loc, dest_loc):
        """
        Sets next location to drive to and duration needed.
        """
        self.rider_loc = rider_loc
        self.dest_loc = dest_loc

            
    def oos_drive_to_rider(self):
        """
        Out-of-service drive to pickup rider
        """
        self.last_coming_from = self.last_heading_to
        loc, self.last_heading_to, dur = self.rider_loc
        yield self.env.timeout(dur)
        self.curr_pos = loc
        self.oos_drive += dur
        if self.verbose:
            print(f'{cdate(self.env.now)}: Driver {self.num:5.0f} OOS-drive: TAZ {self.curr_pos} -> TAZ {loc}')

    
    def complete_trip(self):
        """
        Drive with passenger to location
        """
        self.last_coming_from = self.last_heading_to
        loc, self.last_heading_to, dur = self.dest_loc
        yield self.env.timeout(dur)
        self.curr_pos = loc
        self.trip_total += dur
        self.ontrip = False
        self.num_trips += 1
        if self.verbose:
            print(f'{cdate(self.env.now)}: Driver {self.num:5.0f} completed trip @ TAZ {loc}')