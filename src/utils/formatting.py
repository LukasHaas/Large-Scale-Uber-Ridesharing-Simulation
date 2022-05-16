from datetime import datetime, timedelta
from src.simulation.params import START_DATE

FORMAT_STR = '%a %H:%M:%S'

def cdate(time: int, start_date: datetime=START_DATE, format_str: str=FORMAT_STR) -> str:
    """Pretty prints date string for simulation.

    Args:
        time (int): current environment time
        start_date (datetime, optional): simulation start date. Defaults to START_DATE.
        format_str (str, optional): pretty print format. Defaults to FORMAT_STR.

    Returns:
        str: pretty printed date
    """
    seconds = time - int(time)
    minutes = time % 60
    hour = time / 60
    hour_of_day = int(hour % 24)
    day = int((time / 60) / 24)
    delta = timedelta(days=day, hours=hour_of_day, minutes=minutes, seconds=seconds)    
    date = start_date + delta
    str_date = date.strftime(format_str)
    return str_date