from multiprocessing.pool import RUN
from typing import List
import simpy
from src.simulation.algorithms import GreedyMatcher
from src.simulation.matcher import IncrementalMatcher
from src.simulation.arrivals import RiderProcess, DriverProcess
from src.simulation.params import RUN_DELTA, INITIAL_TIME, INITIAL_DRIVERS, VERBOSE, DEBUG

if __name__ == "__main__":
    # Analysis Containers
    request_collection = []
    driver_collection = []
    trip_collection = []

    # creates a SimPy Environment
    env = simpy.Environment(initial_time=INITIAL_TIME)

    # create store for available drivers and riders
    store = simpy.FilterStore(env, capacity=simpy.core.Infinity)
    
    # instantiate matching algorithm
    algorithm = GreedyMatcher(distance_based=False)
    matcher = IncrementalMatcher(env, algorithm, store, trip_collection, VERBOSE)
    env.process(matcher.perform_matching())

    # driver arrival process (fixed number of drivers)
    driver_process = DriverProcess(env, store, driver_collection, VERBOSE, DEBUG)
    driver_process.run(INITIAL_DRIVERS)
    
    # rider arrival process
    rider_process = RiderProcess(env, store, request_collection, arrival_df, VERBOSE, DEBUG)
    env.process(rider_process.run())

    # Run simulation
    env.run(until=INITIAL_TIME + RUN_DELTA)
