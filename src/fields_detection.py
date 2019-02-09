from TaxiType import TaxiType
# import config


# WIP: Insert missing fields if needed in queries
# Non-existent fields for some period must be added and mapped to the empty string

# Initializes the fields for data of type 'yellow'.
def __init_yellow(period):
    print("yellow " + period)
    fields = {}

    # 2009-01 - 2009-12
    if '2009-01' <= period <= '2009-12':
        fields['pu_lon'] = 'Start_Lon'
        fields['pu_lat'] = 'Start_Lat'
        fields['do_lon'] = 'End_Lon'
        fields['do_lat'] = 'End_Lat'
        fields['pu_loc'] = ''
        fields['do_loc'] = ''
        fields['do_time'] = 'Trip_Dropoff_Datetime'
        fields['fare'] = 'Fare_Amt'
        fields['tip'] = 'Tip_Amt'
        fields['passengers'] = 'Passenger_Count'
        fields['dist'] = 'Trip_Distance'

    # 2010-01 - 2013-12
    if '2010-01' <= period <= '2013-12':
        fields['pu_lon'] = 'pickup_longitude'
        fields['pu_lat'] = 'pickup_latitude'
        fields['do_lon'] = 'dropoff_longitude'
        fields['do_lat'] = 'dropoff_latitude'
        fields['pu_loc'] = ''
        fields['do_loc'] = ''
        fields['pu_time'] = 'pickup_datetime'
        fields['do_time'] = 'dropoff_datetime'
        fields['fare'] = 'fare_amount'
        fields['tip'] = 'tip_amount'
        fields['passengers'] = 'passenger_count'
        fields['dist'] = 'trip_distance'

    # 2014-01 - 2014-12
    if '2014-01' <= period <= '2014-12':
        fields['pu_lon'] = 'pickup_longitude'
        fields['pu_lat'] = 'pickup_latitude'
        fields['do_lon'] = 'dropoff_longitude'
        fields['do_lat'] = 'dropoff_latitude'
        fields['pu_loc'] = ''
        fields['do_loc'] = ''
        fields['do_time'] = 'pickup_datetime'
        fields['do_time'] = 'dropoff_datetime'
        fields['fare'] = 'fare_amount'
        fields['tip'] = 'tip_amount'
        fields['passengers'] = 'passenger_count'
        fields['dist'] = 'trip_distance'

    # 2015-01 - 2016-06
    if '2015-01' <= period <= '2016-06':
        fields['pu_lon'] = 'pickup_longitude'
        fields['pu_lat'] = 'pickup_latitude'
        fields['do_lon'] = 'dropoff_longitude'
        fields['do_lat'] = 'dropoff_latitude'
        fields['pu_loc'] = ''
        fields['do_loc'] = ''
        fields['pu_time'] = 'tpep_pickup_datetime'
        fields['do_time'] = 'tpep_dropoff_datetime'
        fields['fare'] = 'fare_amount'
        fields['tip'] = 'tip_amount'
        fields['passengers'] = 'passenger_count'
        fields['dist'] = 'trip_distance'

    # 2016-07 - 2018-12
    if '2016-07' <= period <= '2018-12':
        fields['pu_lon'] = ''
        fields['pu_lat'] = ''
        fields['do_lon'] = ''
        fields['do_lat'] = ''
        fields['pu_loc'] = 'PULocationID'
        fields['do_loc'] = 'DOLocationID'
        fields['pu_time'] = 'tpep_pickup_datetime'
        fields['do_time'] = 'tpep_dropoff_datetime'
        fields['fare'] = 'fare_amount'
        fields['tip'] = 'tip_amount'
        fields['passengers'] = 'passenger_count'
        fields['dist'] = 'trip_distance'

    return fields


# Initializes the fields for data of type 'green'.
def __init_green(period):
    print("green " + period)
    fields = {}

    # 2013-10 - 2014-12
    if '2013-10' <= period <= '2014-12':
        fields['pu_lon'] = 'Pickup_longitude'
        fields['pu_lat'] = 'Pickup_latitude'
        fields['do_lon'] = 'Dropoff_longitude'
        fields['do_lat'] = 'Dropoff_latitude'
        fields['pu_loc'] = ''
        fields['do_loc'] = ''
        fields['pu_time'] = 'Lpep_pickup_datetime'
        fields['do_time'] = 'Lpep_dropoff_datetime'
        fields['fare'] = 'Fare_amount'
        fields['tip'] = 'Tip_amount'
        fields['passengers'] = 'Passenger_count'
        fields['dist'] = 'Trip_distance'

    # 2015-01 - 2016-06
    if '2015-01' <= period <= '2016-06':
        fields['pu_lon'] = 'Pickup_longitude'
        fields['pu_lat'] = 'Pickup_latitude'
        fields['do_lon'] = 'Dropoff_longitude'
        fields['do_lat'] = 'Dropoff_latitude'
        fields['pu_loc'] = ''
        fields['do_loc'] = ''
        fields['pu_time'] = 'Lpep_pickup_datetime'
        fields['do_time'] = 'Lpep_dropoff_datetime'
        fields['fare'] = 'Fare_amount'
        fields['tip'] = 'Tip_amount'
        fields['passengers'] = 'Passenger_count'
        fields['dist'] = 'Trip_distance'

    # 2016-07 - 2018-12
    if '2016-07' <= period <= '2018-12':
        fields['pu_lon'] = ''
        fields['pu_lat'] = ''
        fields['do_lon'] = ''
        fields['do_lat'] = ''
        fields['pu_loc'] = 'PULocationID'
        fields['do_loc'] = 'DOLocationID'
        fields['pu_time'] = 'lpep_pickup_datetime'
        fields['do_time'] = 'lpep_dropoff_datetime'
        fields['fare'] = 'fare_amount'
        fields['tip'] = 'tip_amount'
        fields['passengers'] = 'passenger_count'
        fields['dist'] = 'trip_distance'

    return fields


# Initializes the fields for data of type 'fhv'.
def __init_fhv(period):
    print("fhv " + period)
    fields = {}

    # 2015-01 - 2016-12
    if '2015-01' <= period <= '2016-12':
        fields['pu_lon'] = ''
        fields['pu_lat'] = ''
        fields['do_lon'] = ''
        fields['do_lat'] = ''
        fields['pu_loc'] = 'locationID'
        fields['do_loc'] = 'locationID'
        fields['pu_time'] = 'Pickup_date'
        fields['do_time'] = ''
        fields['fare'] = ''
        fields['tip'] = ''

    # 2017-01 - 2017-06
    if '2017-01' <= period <= '2017-06':
        fields['pu_lon'] = ''
        fields['pu_lat'] = ''
        fields['do_lon'] = ''
        fields['do_lat'] = ''
        fields['pu_loc'] = 'PULocationID'
        fields['do_loc'] = 'DOLocationID'
        fields['do_time'] = 'Pickup_DateTime'
        fields['do_time'] = 'DropOff_datetime'
        fields['fare'] = ''
        fields['tip'] = ''

    # 2017-07 - 2018-12
    if '2017-07' <= period <= '2018-12':
        fields['pu_lon'] = ''
        fields['pu_lat'] = ''
        fields['do_lon'] = ''
        fields['do_lat'] = ''
        fields['pu_loc'] = 'PULocationID'
        fields['do_loc'] = 'DOLocationID'
        fields['do_time'] = 'Pickup_Datetime'
        fields['do_time'] = 'DropOff_datetime'
        fields['fare'] = ''
        fields['tip'] = ''

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
