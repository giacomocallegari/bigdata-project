from enum import Enum

class TimeScale(Enum):
    NONE = 0
    HOUR = 1
    DAY = 2
    WEEKDAY = 3
    MONTH = 4
    YEAR = 5

scale_names = ['none', 'hour', 'day', 'weekday', 'month', 'year']