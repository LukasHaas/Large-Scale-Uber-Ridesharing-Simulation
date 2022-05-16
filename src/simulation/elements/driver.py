from typing import List
import numpy as np
import pandas as pd
import simpy
from simpy.core import Environment
from simpy.resources.store import FilterStore
from src.utils import cdate
from src.simulation.params import INITIAL_TIME

class Driver(object):
    def __init__(self, num: int, trip_endpoint_data: pd.DataFrame, env: Environment, driver_store: FilterStore,
                 driver_collection: List, verbose: bool=True):
        """Instantiates a driver element for the simulation.

        Args:
            num (int): unique driver number
            env (Environment): simpy environment
            driver_store (FilterStore): container of all available drivers
            driver_collection (List): list of all drivers
            verbose (bool, optional): verbose setting. Defaults to True.
        """
        self.num = num
        self.env = env
        self.driver_store = driver_store
        self.verbose = verbose
        
        # Determine hour of day and weekday
        hour = env.now / 60
        hour_of_day = int(hour % 24)
        weekday = int((env.now / 60 / 24) % 7)
        
        # Sample starting position
        probs = trip_endpoint_data.loc[(weekday, hour_of_day)]['dropoffs'] / trip_endpoint_data.loc[(weekday, hour_of_day)]['dropoffs'].sum()
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
        if self.env.now > INITIAL_TIME and self.verbose:
            print(f'{cdate(self.env.now)}: New Driver {self.num:5.0f} @ TAZ {self.start_pos}')
        
        while True:
            
            # Signal availability
            self.driver_store.put((self.env.now, self))
            if self.num_trips > 0 and self.verbose:
                print(f'{cdate(self.env.now)}: Driver {self.num:5.0f} ready again @ TAZ {self.curr_pos}')
            
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
            yield self.env.process(self.complete_trip())


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
        if wait_time == self.patience:
            self.__available = False
            if self.verbose:
                print(f'{cdate(self.env.now)}: Driver {self.num:5.0f} waited too long -> offline')
            
            
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
        loc, dur = self.rider_loc
        yield self.env.timeout(dur)
        self.curr_pos = loc
        self.oos_drive += dur
        if self.verbose:
            print(f'{cdate(self.env.now)}: Driver {self.num:5.0f} OOS-drive: TAZ {self.curr_pos} -> TAZ {loc}')

    
    def complete_trip(self):
        """
        Drive with passenger to location
        """
        loc, dur = self.dest_loc
        yield self.env.timeout(dur)
        self.curr_pos = loc
        self.trip_total += dur
        self.ontrip = False
        self.num_trips += 1
        if self.verbose:
            print(f'{cdate(self.env.now)}: Driver {self.num:5.0f} completed trip @ TAZ {loc}')