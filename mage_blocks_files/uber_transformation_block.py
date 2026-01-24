import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    datetime_dim = df[['tpep_pickup_datetime', 'tpep_dropoff_datetime']].drop_duplicates().reset_index(drop=True)   #Droped the duplicates
    datetime_dim['pickup_hour'] = datetime_dim['tpep_pickup_datetime'].dt.hour
    datetime_dim['pickup_day'] = datetime_dim['tpep_pickup_datetime'].dt.day
    datetime_dim['pickup_month'] = datetime_dim['tpep_pickup_datetime'].dt.month
    datetime_dim['pickup_year'] = datetime_dim['tpep_pickup_datetime'].dt.year
    datetime_dim['pickup_weekday'] = datetime_dim['tpep_pickup_datetime'].dt.weekday

    datetime_dim['drop_hour'] = datetime_dim['tpep_dropoff_datetime'].dt.hour
    datetime_dim['drop_day'] = datetime_dim['tpep_dropoff_datetime'].dt.day
    datetime_dim['drop_month'] = datetime_dim['tpep_dropoff_datetime'].dt.month
    datetime_dim['drop_year'] = datetime_dim['tpep_dropoff_datetime'].dt.year
    datetime_dim['drop_weekday'] = datetime_dim['tpep_dropoff_datetime'].dt.weekday

    datetime_dim['datetime_id'] = datetime_dim.index
    datetime_dim = datetime_dim[['datetime_id', 'tpep_pickup_datetime', 'pickup_hour', 'pickup_day', 'pickup_month', 'pickup_year', 'pickup_weekday', 'tpep_dropoff_datetime', 'drop_hour','drop_day', 'drop_month', 'drop_year', 'drop_weekday']]

    passenger_count_dim = df[['passenger_count']].drop_duplicates().reset_index(drop=True)
    passenger_count_dim['passenger_count_id'] = passenger_count_dim.index
    passenger_count_dim = passenger_count_dim[['passenger_count_id', 'passenger_count']]

    trip_distance_dim = df[['trip_distance']].drop_duplicates().reset_index(drop=True)
    trip_distance_dim['trip_distance_id'] = trip_distance_dim.index
    trip_distance_dim = trip_distance_dim[['trip_distance_id', 'trip_distance']]

    rate_code_type = { 
        1: "standard rate",
        2: "jfk",
        3: "newark",
        4: "nassau or westchester",
        5: "negotiated fare",
        6: "group ride",
    }
    ratecode_dim = df[['RatecodeID']].drop_duplicates().reset_index(drop=True)
    ratecode_dim['rate_code_id'] = ratecode_dim.index
    ratecode_dim['ratecode_name'] = ratecode_dim['RatecodeID'].map(rate_code_type)
    ratecode_dim = ratecode_dim[['rate_code_id', 'RatecodeID', 'ratecode_name']]

    pickup_location_dim = df[['pickup_longitude', 'pickup_latitude']].drop_duplicates().reset_index(drop=True)
    pickup_location_dim['pickup_location_id'] = pickup_location_dim.index
    pickup_location_dim = pickup_location_dim[['pickup_location_id', 'pickup_longitude', 'pickup_latitude']]

    dropoff_location_dim = df[['dropoff_longitude', 'dropoff_latitude']].drop_duplicates().reset_index(drop=True)
    dropoff_location_dim['dropoff_location_id'] = dropoff_location_dim.index
    dropoff_location_dim = dropoff_location_dim[['dropoff_location_id', 'dropoff_longitude', 'dropoff_latitude']]

    payment_type_name = {
        1: "Credit Card",
        2: "Cash",
        3: "No charge",
        4: "Dispute",
        5: "Unknown",
        6: "voided trip"
    }
    payment_type_dim = df[['payment_type']].drop_duplicates().reset_index(drop=True)
    payment_type_dim['payment_type_id'] = payment_type_dim.index
    payment_type_dim['payment_type_name'] = payment_type_dim['payment_type'].map(payment_type_name)
    payment_type_dim = payment_type_dim[['payment_type_id', 'payment_type', 'payment_type_name']]

    fact_table = df.merge(passenger_count_dim, on='passenger_count') \
        .merge(trip_distance_dim, on='trip_distance') \
        .merge(ratecode_dim, on='RatecodeID') \
        .merge(pickup_location_dim, on=['pickup_longitude', 'pickup_latitude']) \
        .merge(dropoff_location_dim, on=['dropoff_longitude', 'dropoff_latitude']) \
        .merge(datetime_dim, on=['tpep_pickup_datetime', 'tpep_dropoff_datetime']) \
        .merge(payment_type_dim, on='payment_type') \
        [['VendorID', 'datetime_id', 'passenger_count_id', 'trip_distance_id', 
        'pickup_location_id', 'dropoff_location_id', 'RatecodeID', 
        'payment_type_id', 'fare_amount', 'mta_tax', 'tip_amount', 
        'tolls_amount', 'improvement_surcharge', 'total_amount']]

    return {
    "datetime_dim": datetime_dim,
    "passenger_count_dim": passenger_count_dim,
    "trip_distance_dim": trip_distance_dim,
    "ratecode_dim": ratecode_dim,
    "pickup_location_dim": pickup_location_dim,
    "dropoff_location_dim": dropoff_location_dim,
    "payment_type_dim": payment_type_dim,
    "fact_table": fact_table,
    }



@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
