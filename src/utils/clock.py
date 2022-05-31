from simpy.core import Environment
from src.utils.formatting import cdate

class Clock(object):
    def __init__(self, env: Environment, interval: float):
        self.env = env
        self.interval = interval

    def run(self):
        while True:
            yield self.env.timeout(self.interval)
            time_string = cdate(self.env.now)
            print('Current time:', time_string)
