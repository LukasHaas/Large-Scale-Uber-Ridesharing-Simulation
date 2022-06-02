from typing import List
import numpy as np
import pandas as pd
import random
import simpy
from simpy.core import Environment
from simpy.resources.store import FilterStore
from src.utils import cdate
from src.simulation.params import INITIAL_TIME, MAX_DRIVER_JOB_QUEUE
from src.utils.sampling import sample_point_in_geometry

class Driver(object):
    def __init__(self, num: int, trip_endpoint_data: pd.DataFrame, geo_df: pd.DataFrame, num_driver_df: pd.DataFrame, env: Environment,
                 driver_store: FilterStore, driver_collection: List, num_active_drivers: List, num_active_riders: List, verbose: bool=True):
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
            num_active_riders (List): list containing one number which is the current number of active riders
            verbose (bool, optional): verbose setting. Defaults to True.
        """
        self.num = num
        self.env = env
        self.driver_store = driver_store
        self.num_driver_df = num_driver_df
        self.num_active_drivers = num_active_drivers
        self.num_active_riders = num_active_riders
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
        self.num_trips = 0

        # Status flags
        self.online = True
        self.accepting_jobs = True
        self.ontrip = False
        self.is_oos = True
        self.will_head_home = False
        
        # Patience (NONE for infinity)
        self.patience = None

        # Last known location
        self.last_coming_from = sample_point_in_geometry(geo_df.loc[self.start_pos]['geometry'], 1)
        self.last_heading_to = self.last_coming_from
        
        # Job queue
        self.jobs = []
        self.curr_job = None
        
        # Save driver for analysis
        driver_collection.append(self)
        
        # start the drive process when instance is created
        self.action = env.process(self.drive())
        
    @property
    def available(self):
        return self.online and self.accepting_jobs and self.will_head_home == False
        
    @property
    def offline(self):
        return self.online == False
        
    @property
    def oos_total(self):
        return self.oos_wait + self.oos_drive
    
    @property
    def total_time_active(self):
        return self.oos_wait + self.oos_drive + self.trip_total

    @property
    def num_jobs(self):
        n = len(self.jobs)
        if self.curr_job is not None:
            n += 1

        return n

    @property
    def exp_time_to_availability(self):
        if self.num_jobs == 0:
            return 0

        time_needed = np.sum([job.expected_time for job in self.jobs])
        if self.curr_job is not None:
            time_needed += self.curr_job.exp_time_to_completion

        return time_needed

    @property
    def anticipated_pos(self):
        if self.num_jobs == 0:
            return self.curr_pos
        elif self.curr_job is not None:
            return self.curr_job.to_dest.taz
        else:
            return self.jobs[-1].to_dest.taz


    def drive(self):
        """
        Driver starts servicing requests.
        """
        # Go online on app
        self.go_online()
        if self.env.now > INITIAL_TIME and self.verbose:
            print(f'{cdate(self.env.now)}: Driver {self.num:5.0f} dispatched @ TAZ {self.start_pos}. Active drivers: {self.num_active_drivers[0]:,}')
        
        while self.online:
            
            # Signal not on trip with rider right now
            self.is_oos = True
            
            # Wait for request if job queue is empty
            if self.num_jobs == 0:
                try:
                    yield self.env.process(self.wait_for_request())
                except simpy.Interrupt:
                    if self.verbose:
                        print(f'{cdate(self.env.now)}: Driver {self.num:5.0f} got request, waited for {self.oos_wait:2.2f} @ TAZ {self.curr_pos}')
            
            # Get job
            self.curr_job = self.jobs.pop(0)
            
            # Drive to rider
            yield self.env.process(self.oos_drive_to_rider(self.curr_job))
            
            # Complete trip
            yield self.env.process(self.complete_trip(self.curr_job))

            # Decide if should head home
            self.should_head_home()
                
            # Update accepting jobs
            self.update_accepting_jobs_status()
            
            # Go offline if necessary
            if self.num_jobs == 0 and self.will_head_home:
                self.go_offline()
                if self.verbose:
                    print(f'{cdate(self.env.now)}: Driver {self.num:5.0f} is heading home. Going offline. Active drivers: {self.num_active_drivers[0]:,}')

    
    def update_accepting_jobs_status(self):
        """Updates whether driver can accept further jobs.
        """
        if self.will_head_home:
            self.accepting_jobs = False
            return
        
        self.accepting_jobs = (self.num_jobs < MAX_DRIVER_JOB_QUEUE)
        if self.accepting_jobs:
            self.driver_store.put((self.env.now, self))


    def go_offline(self):
        """Sets driver to offline.
        """
        self.online = False

    
    def go_online(self):
        """Sets driver to online.
        """
        self.num_active_drivers[0] += 1
        self.online = True
        
        # Signal availability
        self.driver_store.put((self.env.now, self))

    
    def should_head_home(self):
        """Process for driver to decide if heading home

        Note: the process depends on how close the current number of online drivers
              matches the overall average number of online drivers at that time.
        """
        # TODO: anticipated drivers seems to be too high
        # Get target supply
        hour = (self.env.now / 60)
        hour_of_day = int(hour % 24)
        minute = int(self.env.now % 60)
        target_uber_supply = self.num_driver_df.loc[(hour_of_day, minute), 'n_drivers']

        # Decide if to go home
        ratio = self.num_active_drivers[0] / self.num_active_riders[0]
        if ratio > 1.25:
            # Overwrite probability if supply far outstrips demand (> 125% ratio)
            probability = min(1, ratio - 1)
        elif ratio > 0.9:
            # Calculate base probability based on TNC supply patterns
            surplus = self.num_active_drivers[0] - target_uber_supply
            probability = min(1, surplus / (self.num_active_drivers[0] / 7.5)) # surplus should be gone within 8 minutes
        else:
            return

        # Head home if makes sense to do so
        if self.num_trips > 0 and random.random() < probability:
            self.will_head_home = True


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


    def accept_job(self, job):
        """Utility method for drivers accepting jobs.

        Args:
            job (Job): driving job to accept.
        """
        self.jobs.append(job)
        self.update_accepting_jobs_status()
        if self.verbose:
            print(f'{cdate(self.env.now)}: Driver {self.num:5.0f} accepted job # {self.num_jobs}: TAZ {job.to_rider.taz} -> TAZ {job.to_dest.taz}')

            
    def oos_drive_to_rider(self, job):
        """
        Out-of-service drive to pickup rider
        """
        # Update headings
        self.last_coming_from = self.last_heading_to
        self.last_heading_to = job.to_rider.point

        # Drive
        self.ontrip = True
        job.start()
        yield self.env.timeout(job.to_rider.time)

        # Update flags and analytics
        self.curr_pos = job.to_rider.taz
        self.oos_drive += job.to_rider.time
        self.is_oos = False
        if self.verbose:
            print(f'{cdate(self.env.now)}: Driver {self.num:5.0f} OOS-drive: TAZ {self.curr_pos} -> TAZ {job.to_rider.taz}')

    
    def complete_trip(self, job):
        """
        Drive with passenger to location
        """
        # Upodate headings
        self.last_coming_from = self.last_heading_to
        self.last_heading_to = job.to_dest.point

        # Drive
        yield self.env.timeout(job.to_dest.time)

        # Update flags and analytics
        self.curr_job = None
        self.curr_pos = job.to_dest.taz
        self.ontrip = False
        self.trip_total += job.to_dest.time
        self.num_trips += 1

        if self.verbose:
            print(f'{cdate(self.env.now)}: Driver {self.num:5.0f} completed trip @ TAZ {job.to_dest.taz}')