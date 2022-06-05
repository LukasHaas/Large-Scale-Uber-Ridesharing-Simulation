from typing import List
from simpy.core import Environment
from src.utils.formatting import cdate

KEPLER_STR = '%Y/%m/%d %H:%M:%S'

class Clock(object):
    def __init__(self, env: Environment, num_active_drivers: List,
                 num_active_requests: List, interval: float):
        self.env = env
        self.interval = interval
        self.__num_active_drivers = num_active_drivers
        self.__num_active_requests = num_active_requests
        self.data = []

    def run(self):
        while True:
            yield self.env.timeout(self.interval)
            time_string = cdate(self.env.now)
            datetime = cdate(self.env.no, format_str=KEPLER_STR)
            ratio = (100 * self.num_active_drivers) / self.num_active_requests
            self.data.append([datetime, self.num_active_drivers, self.num_active_requests, ratio])
            print(f'{time_string}: Active drivers: {self.num_active_drivers:,} <> {self.num_active_requests:,} active riders/requests. Ratio: {ratio:.1f} %')

    @property
    def num_active_drivers(self):
        return self.__num_active_drivers[0]

    @property
    def num_active_requests(self):
        return self.__num_active_requests[0]
