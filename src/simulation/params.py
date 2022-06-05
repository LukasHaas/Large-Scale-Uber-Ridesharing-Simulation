from datetime import datetime

# Simulation parameters
UBER_MARKET_SHARE = 0.8
MIN_TRIP_TIME = 1.
START_DATE = datetime(year=2016, month=12, day=5)
INITIAL_DRIVERS = None
INITIAL_TIME = 3 * 24 * 60 + 0 * 60  # Wednesday @ midnight
RUN_DELTA = 60 * 2
BATCH_FREQUENCY = 1. / 3 # 20 seconds interval matching
MAX_DRIVER_JOB_QUEUE = 2
DYNAMIC_SUPPLY = True
MARKET_FORCE_SUPPLY = False # TODO: Implement working DYNAMIC_SUPPLY = True mode

# Output control
FUNCTION_TIMING = False
VERBOSE = False
DEBUG = False
STALL_DRIVERS = False
CLOCK_LOG_TIME = 1

# Files
ARRIVAL_PATH = '/Users/lukashaas/Documents/Stanford/4 Senior/3 Spring/MS&E 230/Project/Code/data/processed_rider_arrival_rates.csv'
DRIVER_PATH = '/Users/lukashaas/Documents/Stanford/4 Senior/3 Spring/MS&E 230/Project/Code/data/processed_driver_arrivals.csv'
TRAVEL_TIMES_PATH = '/Users/lukashaas/Documents/Stanford/4 Senior/3 Spring/MS&E 230/Project/Code/data/processed_uber_time_data.csv'
PICKUP_DROPOFF_PATH = '/Users/lukashaas/Documents/Stanford/4 Senior/3 Spring/MS&E 230/Project/Code/data/processed_pickups_dropoffs.csv'
TAZ_GEOMETRY_PATH = '/Users/lukashaas/Documents/Stanford/4 Senior/3 Spring/MS&E 230/Project/Code/data/processed_taz_geometries.csv'