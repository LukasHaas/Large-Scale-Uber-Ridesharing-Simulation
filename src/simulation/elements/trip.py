from typing import List
from simpy.core import Environment
from src.utils import sample_random_trip_time
from .driver import Driver
from .rider import Rider
from src.utils import cdate

class Trip(object):
    def __init__(self, env: Environment, rider: Rider, driver: Driver,
                 trip_collection: List, verbose: bool=True):
        """
        Trip class which performs trips and saves information.
        """
        self.env = env
        self.rider = rider
        self.driver = driver
        self.verbose = verbose
        
        # Determine hour of day and weekday
        self.start_time = env.now
        self.hour_of_day = int((env.now / 60) % 24)
        
        # Calculate time needed for getting to rider
        self.time_to_rider = sample_random_trip_time(self.hour_of_day, driver.curr_pos, rider.pos)
        
        # Calculate time needed for trip (look ahead)
        hour_of_day_trip = int(((env.now + self.time_to_rider) / 60) % 24)
        self.time_to_destination = sample_random_trip_time(hour_of_day_trip, rider.pos, rider.des, is_trip=True)
        
        # Save trip for analysis
        trip_collection.append(self)
    
    @property
    def time_to_completion(self):
        return self.time_to_rider + self.time_to_destination
    
    def perform(self):
        """
        Performs the trip by giving necessary information to both parties.
        """
        # Communicate information
        rider_loc = (self.rider.pos, self.rider.pos_point, self.time_to_rider)
        trip_loc = (self.rider.des, self.rider.des_point, self.time_to_destination)
        self.driver.set_next_destination(rider_loc, trip_loc)
        self.rider.set_next_trip_duration(self.time_to_rider, self.time_to_destination)
        
        # Wake up parties
        if self.verbose:
            print(f'{cdate(self.env.now)}: Trip communicated (Driver: {self.driver.num}, Rider: {self.rider.num})')
        self.driver.action.interrupt()
        self.rider.action.interrupt()