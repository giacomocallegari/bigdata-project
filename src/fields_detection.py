from TaxiType import TaxiType
# from location_conversion import coordinates_to_zone as ctz
# import config


# WIP: Insert missing fields if needed in queries
# WIP: Coordinate conversion not managed yet

# Initializes the fields for data of type 'yellow'.
def __init_yellow(period):
    print("yellow " + period)
    fields = {}

    # 2009-01 - 2009-10
    if '2009-01' <= period <= '2009-10':
        fields['pu_loc'] = ''  # Coordinates
        fields['do_loc'] = ''  # Coordinates
        fields['do_time'] = 'Trip_Dropoff_Datetime'
        fields['tip'] = 'Tip_Amt'

    # 2010-01 - 2013-10
    if '2010-01' <= period <= '2013-10':
        fields['pu_loc'] = ''  # Coordinates
        fields['do_loc'] = ''  # Coordinates
        fields['do_time'] = 'dropoff_datetime'
        fields['tip'] = 'tip_amount'

    # 2014-01 - 2014-10
    if '2014-01' <= period <= '2014-10':
        fields['pu_loc'] = ''  # Coordinates
        fields['do_loc'] = ''  # Coordinates
        fields['do_time'] = 'dropoff_datetime'
        fields['tip'] = 'tip_amount'

    # 2015-01 - 2016-04
    if '2015-01' <= period <= '2016-04':
        fields['pu_loc'] = ''  # Coordinates
        fields['do_loc'] = ''  # Coordinates
        fields['do_time'] = 'tpep_dropoff_datetime'
        fields['tip'] = 'tip_amount'

    # 2016-07 - 2018-04
    if '2016-07' <= period <= '2018-04':
        fields['pu_loc'] = 'PULocationID'
        fields['do_loc'] = 'DOLocationID'
        fields['do_time'] = 'tpep_dropoff_datetime'
        fields['tip'] = 'tip_amount'

    return fields


# Initializes the fields for data of type 'green'.
def __init_green(period):
    print("green " + period)
    fields = {}

    # 2013-10 - 2014-10
    if '2013-10' <= period <= '2014-10':
        fields['pu_loc'] = ''  # Coordinates
        fields['do_loc'] = ''  # Coordinates
        fields['do_time'] = 'Lpep_dropoff_datetime'
        fields['tip'] = 'Tip_amount'

    # 2015-01 - 2016-04
    if '2015-01' <= period <= '2016-04':
        fields['pu_loc'] = ''  # Coordinates
        fields['do_loc'] = ''  # Coordinates
        fields['do_time'] = 'Lpep_dropoff_datetime'
        fields['tip'] = 'Tip_amount'

    # 2016-07 - 2018-04
    if '2016-07' <= period <= '2018-04':
        fields['pu_loc'] = 'PULocationID'
        fields['do_loc'] = 'DOLocationID'
        fields['do_time'] = 'lpep_dropoff_datetime'
        fields['tip'] = 'tip_amount'

    return fields


# Initializes the fields for data of type 'fhv'.
def __init_fhv(period):
    print("fhv " + period)
    fields = {}

    # 2015-01 - 2016-10
    if '2015-01' <= period <= '2016-10':
        fields['pu_loc'] = 'locationID'
        fields['do_loc'] = 'locationID'

    # 2017-01 - 2017-04
    if '2017-01' <= period <= '2017-04':
        fields['pu_loc'] = 'PULocationID'
        fields['do_loc'] = 'DOLocationID'
        fields['do_time'] = 'DropOff_datetime'

    # 2017-07 - 2018-04
    if '2017-07' <= period <= '2018-04':
        fields['pu_loc'] = 'PULocationID'
        fields['do_loc'] = 'DOLocationID'
        fields['do_time'] = 'DropOff_datetime'

    return fields


# Initializes the fields that will be used in queries.
def init_fields(file_type, period):
    fields = {}

    if file_type == TaxiType.YELLOW:
        fields = __init_yellow(period)
    elif file_type == TaxiType.GREEN:
        fields = __init_green(period)
    elif file_type == TaxiType.FHV:
        fields = __init_fhv(period)
    else:
        print("invalid type")

    return fields
