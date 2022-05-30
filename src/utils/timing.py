from functools import wraps
from time import time
from src.simulation.params import FUNCTION_TIMING

def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        if FUNCTION_TIMING:
            print(f'func:{f.__name__} took: {te-ts:2.4f} secs')
            
        return result
    return wrap