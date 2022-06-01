import pandas as pd
import geopandas as gpd
import simpy
from src.utils import Clock
from src.simulation.algorithms import GreedyMatcher, ShortestDistance
from src.simulation.matcher import IncrementalMatcher, BatchMatcher
from src.simulation.arrivals import RiderProcess, DriverProcess
from src.simulation.matcher.batch_matcher import BatchMatcher
from src.simulation.monitoring import save_run, DriverAnalytics
from src.simulation.params import RUN_DELTA, INITIAL_TIME, INITIAL_DRIVERS, VERBOSE, \
                                  DEBUG, ARRIVAL_PATH, TRAVEL_TIMES_PATH, \
                                  TAZ_GEOMETRY_PATH, BATCH_FREQUENCY, CLOCK_LOG_TIME

if __name__ == "__main__":
    # Analysis Containers
    request_collection = []
    driver_collection = []
    trip_collection = []

    # Load relevant data
    arrival_df = pd.read_csv(ARRIVAL_PATH, index_col=['day_of_week', 'hour', 'minute'])['pickups']
    travel_time_df = pd.read_csv(TRAVEL_TIMES_PATH, index_col=['hod', 'sourceid', 'dstid'])
    geo_df = pd.read_csv(TAZ_GEOMETRY_PATH, index_col=['MOVEMENT_ID_uber'])
    geo_df['geometry'] = gpd.GeoSeries.from_wkt(geo_df['geometry'])

    # creates a SimPy Environment
    env = simpy.Environment(initial_time=INITIAL_TIME)

    # create store for available drivers and riders
    store = simpy.FilterStore(env, capacity=simpy.core.Infinity)
    
    # instantiate matching algorithm
    if BATCH_FREQUENCY is None:
        algorithm = GreedyMatcher(uber_data=travel_time_df, distance_based=False)
        matcher = IncrementalMatcher(env, algorithm, store, trip_collection, VERBOSE)
    else:
        algorithm = ShortestDistance(uber_data=travel_time_df)
        matcher = BatchMatcher(env, algorithm, BATCH_FREQUENCY, store, trip_collection, VERBOSE)
        env.process(matcher.keep_running_availabilities())

    env.process(matcher.perform_matching())

    # driver arrival process
    num_active_drivers = [0]
    anticipated_active_drivers = [0]
    driver_process = DriverProcess(env, store, driver_collection, INITIAL_DRIVERS, \
                                   num_active_drivers, anticipated_active_drivers, geo_df, VERBOSE, DEBUG)
    env.process(driver_process.run())
    
    # rider arrival process
    rider_process = RiderProcess(env, store, request_collection, arrival_df, geo_df, VERBOSE, DEBUG)
    env.process(rider_process.run())

    # driver analytics
    da = DriverAnalytics(env, driver_collection)
    env.process(da.analyse())

    # clock
    if CLOCK_LOG_TIME is not None:
        clock = Clock(env, num_active_drivers, CLOCK_LOG_TIME)
        env.process(clock.run())

    # Run simulation
    env.run(until=INITIAL_TIME + RUN_DELTA)

    # Save simulation data
    save_run(request_collection, da, geo_df)
