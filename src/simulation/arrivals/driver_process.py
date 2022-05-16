from .arrival_process import ArrivalProcess
from src.simulation.elements import Driver

class DriverProcess(ArrivalProcess):
    def run(self, initial_drivers: int):
        """
        Simulates the arrival process of drivers throughout the city.
        """
        driver_number = 0
        _ = [Driver(i, self.env, self.store, self.collection, self.verbose) for i in range(initial_drivers)]
        if self.verbose:
            print(f'Spawned {initial_drivers:,} drivers')