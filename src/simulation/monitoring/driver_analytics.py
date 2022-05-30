from typing import List
from simpy.core import Environment
from src.utils.formatting import cdate

KEPLER_STR = '%d%m%y %H:%M:%S'

class DriverAnalytics(object):
    def __init__(self, env: Environment, driver_collection: List):
        self.env = env
        self.driver_collection = driver_collection
        self.analytics = []

    def analyse(self, period: int=5):
        while True:
            yield self.env.timeout(period)
            self.gather_driver_information()
            
    def gather_driver_information(self):
        """Generate snapshot of driver information.
        """
        datetime = cdate(self.env.now, format_str=KEPLER_STR)
        date = datetime.split()[0]
        time = datetime.split()[1]
        for driver in self.driver_collection:
            if driver.offline:
                continue

            from_lon = driver.last_coming_from.coords.xy[0][0]
            from_lat = driver.last_coming_from.coords.xy[1][0]
            to_lon = driver.last_heading_to.coords.xy[0][0]
            to_lat = driver.last_heading_to.coords.xy[1][0]

            driver_data = [date, time, driver.num, from_lon, from_lat, to_lon, to_lat, driver.is_oos, driver.ontrip]
            self.analytics.append(driver_data)
