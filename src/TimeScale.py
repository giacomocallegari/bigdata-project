from enum import Enum

class TimeScale(Enum):
    NONE = 0
    HOUR = 1
    DAY = 2
    MONTH = 3
    YEAR = 4

scale_names = ['none', 'hour', 'day', 'month', 'year']