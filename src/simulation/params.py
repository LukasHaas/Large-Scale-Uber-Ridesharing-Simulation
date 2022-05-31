from datetime import datetime

# Simulation parameters
UBER_MARKET_SHARE = 0.8
MIN_TRIP_TIME = 1.
START_DATE = datetime(year=2016, month=12, day=5)
INITIAL_DRIVERS = 2500 # 200
INITIAL_TIME = 0 * 24 * 60 + 12 * 60  # Monday @ noon
RUN_DELTA = 60 * 24 * 7 # Full week
BATCH_FREQUENCY = 1. / 3

# Output control
FUNCTION_TIMING = False
VERBOSE = True #True
DEBUG = False # True
CLOCK_LOG_TIME = 60

# Files
ARRIVAL_PATH = '/Users/lukashaas/Documents/Stanford/4 Senior/3 Spring/MS&E 230/Project/Code/data/processed_rider_arrival_rates.csv'
TRAVEL_TIMES_PATH = '/Users/lukashaas/Documents/Stanford/4 Senior/3 Spring/MS&E 230/Project/Code/data/processed_uber_time_data.csv'
# PROXY_TIMES_PATH = '/Users/lukashaas/Documents/Stanford/4 Senior/3 Spring/MS&E 230/Project/Code/data/processed_uber_time_proxy_data.csv'
PICKUP_DROPOFF_PATH = '/Users/lukashaas/Documents/Stanford/4 Senior/3 Spring/MS&E 230/Project/Code/data/processed_pickups_dropoffs.csv'
TAZ_GEOMETRY_PATH = '/Users/lukashaas/Documents/Stanford/4 Senior/3 Spring/MS&E 230/Project/Code/data/processed_taz_geometries.csv'