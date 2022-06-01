from typing import List
from simpy.core import Environment
from .driver import Driver
from .rider import Rider
from .job import Job
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

        # Create job
        self.job = Job(env, rider, driver)
        
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
        self.driver.accept_job(self.job)
        self.rider.set_trip_duration(self.job)
        
        # Wake up parties as required
        self.rider.action.interrupt()
        if self.driver.num_jobs == 1 and self.driver.curr_job is None:
            self.driver.action.interrupt()

        if self.verbose:
            print(f'{cdate(self.env.now)}: Trip communicated (Driver: {self.driver.num}, Rider: {self.rider.num})')