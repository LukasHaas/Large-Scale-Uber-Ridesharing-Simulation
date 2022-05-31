from typing import List
from simpy.core import Environment
from src.utils.formatting import cdate

class Clock(object):
    def __init__(self, env: Environment, num_active_drivers: List, interval: float):
        self.env = env
        self.interval = interval
        self.__num_active_drivers = num_active_drivers

    def run(self):
        while True:
            yield self.env.timeout(self.interval)
            time_string = cdate(self.env.now)
            print(f'{time_string}: Active drivers: {self.num_active_drivers:,}') # <> {self.num_active_requests}')

    @property
    def num_active_drivers(self):
        return self.__num_active_drivers[0]
