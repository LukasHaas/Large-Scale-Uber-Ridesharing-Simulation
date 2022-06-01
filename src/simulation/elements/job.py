from collections import namedtuple
from simpy.core import Environment
from .driver import Driver
from src.utils import sample_random_trip_time

TripLeg = namedtuple('TripLeg', 'taz point time exp_time')

class Job(object):
    def __init__(self, env: Environment, rider, driver: Driver):
        self.env = env
        self.exp_completion = None

        # Calculate time needed for getting to rider
        hour_of_day = int((env.now / 60) % 24)
        time_to_rider, exp_time_to_rider = sample_random_trip_time(hour_of_day, driver.curr_pos, \
                                                                   rider.pos, get_expected=True)
        # Calculate time needed for trip (look ahead)
        hour_of_day_trip = int(((env.now + exp_time_to_rider) / 60) % 24)
        time_to_destination, exp_to_destination = sample_random_trip_time(hour_of_day_trip, rider.pos, rider.des, \
                                                                          is_trip=True, get_expected=True)

        self.to_rider = TripLeg(rider.pos, rider.pos_point, time_to_rider, exp_time_to_rider)
        self.to_dest = TripLeg(rider.des, rider.des_point, time_to_destination, exp_to_destination)

    
    @property
    def actual_time(self):
        return self.to_rider.time + self.to_dest.time

    @property
    def expected_time(self):
        return self.to_rider.exp_time + self.to_dest.exp_time

    def start(self):
        self.exp_completion = self.env.now + self.expected_time
    
    @property
    def exp_time_to_completion(self):
        assert self.exp_completion is not None, 'Expected completion time not set for job.'
        return max(0, self.env.now - self.exp_completion)