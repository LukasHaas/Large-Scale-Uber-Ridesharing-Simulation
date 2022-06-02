from typing import List
from simpy.core import Environment
from src.utils.formatting import cdate

class Clock(object):
    def __init__(self, env: Environment, num_active_drivers: List, num_active_requests: List, interval: float):
        self.env = env
        self.interval = interval
        self.__num_active_drivers = num_active_drivers
        self.__num_active_requests = num_active_requests
        self.active_participants = []

    def run(self):
        while True:
            yield self.env.timeout(self.interval)
            time_string = cdate(self.env.now)
            self.active_participants.append([time_string, self.num_active_drivers, self.num_active_requests])
            print(f'{time_string}: Active drivers: {self.num_active_drivers:,} <> {self.num_active_requests:,} active riders/requests')

    @property
    def num_active_drivers(self):
        return self.__num_active_drivers[0]

    @property
    def num_active_requests(self):
        return self.__num_active_requests[0]
