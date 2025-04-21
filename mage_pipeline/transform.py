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

    RATE_CODE_TYPE = {
    1: "Standard rate",
    2: "JFK",
    3: "Newark",
    4: "Nassau or Westchester",
    5: "Negotiated fare",
    6: "Group ride",
    }

    PAYMENT_TYPE_NAME = {
        1: "Credit card",
        2: "Cash",
        3: "No charge",
        4: "Dispute",
        5: "Unknown",
        6: "Voided trip",
    }

    # Convert to datetime once
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    # --- DIMENSIONS VIA map (no merges) ---

    # 1) Datetime dimension: extract and drop duplicates on-the-fly
    dt = df[['tpep_pickup_datetime', 'tpep_dropoff_datetime']].drop_duplicates()
    dt = dt.assign(
        pick_hour=dt['tpep_pickup_datetime'].dt.hour,
        pick_day=dt['tpep_pickup_datetime'].dt.day,
        pick_month=dt['tpep_pickup_datetime'].dt.month,
        pick_year=dt['tpep_pickup_datetime'].dt.year,
        pick_weekday=dt['tpep_pickup_datetime'].dt.weekday,
        drop_hour=dt['tpep_dropoff_datetime'].dt.hour,
        drop_day=dt['tpep_dropoff_datetime'].dt.day,
        drop_month=dt['tpep_dropoff_datetime'].dt.month,
        drop_year=dt['tpep_dropoff_datetime'].dt.year,
        drop_weekday=dt['tpep_dropoff_datetime'].dt.weekday,
    )
    dt['datetime_id'] = pd.RangeIndex(len(dt))
    datetime_dim = dt[[
        'datetime_id',
        'tpep_pickup_datetime','pick_hour','pick_day','pick_month','pick_year','pick_weekday',
        'tpep_dropoff_datetime','drop_hour','drop_day','drop_month','drop_year','drop_weekday'
    ]]

    dt_key = datetime_dim.set_index(
    ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
    )['datetime_id']

    # Create the same tuple key on df and map it
    df['datetime_id'] = list(
        zip(df['tpep_pickup_datetime'], df['tpep_dropoff_datetime'])
    )
    df['datetime_id'] = df['datetime_id'].map(dt_key)

    # 2) Passenger count (very small): use category codes
    df['passenger_count_id'] = df['passenger_count'].astype('category').cat.codes
    passenger_count_dim = (
        df[['passenger_count_id','passenger_count']]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    # 3) Trip distance: similarly via category
    df['trip_distance_id'] = df['trip_distance'].astype('category').cat.codes
    trip_distance_dim = (
        df[['trip_distance_id','trip_distance']]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    # 4) Rate code: map names directly, then category‐code
    df['rate_code_name'] = df['RatecodeID'].map(RATE_CODE_TYPE)
    df['rate_code_id'] = df['RatecodeID'].astype('category').cat.codes
    rate_code_dim = (
        df[['rate_code_id','RatecodeID','rate_code_name']]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    # 5) Payment type: same pattern
    df['payment_type_name'] = df['payment_type'].map(PAYMENT_TYPE_NAME)
    df['payment_type_id'] = df['payment_type'].astype('category').cat.codes
    payment_type_dim = (
        df[['payment_type_id','payment_type','payment_type_name']]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    # 6) Locations: drop duplicates and assign IDs
    #    You may want to round coordinates to reduce cardinality
    pickup = (
        df[['pickup_latitude','pickup_longitude']]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    pickup['pickup_location_id'] = pickup.index
    pickup_location_dim = pickup[['pickup_location_id','pickup_latitude','pickup_longitude']]

    dropoff = (
        df[['dropoff_latitude','dropoff_longitude']]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dropoff['dropoff_location_id'] = dropoff.index
    dropoff_location_dim = dropoff[['dropoff_location_id','dropoff_latitude','dropoff_longitude']]

    # --- FACT TABLE: assemble IDs without merge ---
    # First, build a key to join back datetime_dim
    pickup_key = pickup_location_dim.set_index(
    ['pickup_latitude', 'pickup_longitude']
    )['pickup_location_id']
    df['pickup_location_id'] = list(
        zip(df['pickup_latitude'], df['pickup_longitude'])
    )
    df['pickup_location_id'] = df['pickup_location_id'].map(pickup_key)

    dropoff_key = dropoff_location_dim.set_index(
        ['dropoff_latitude', 'dropoff_longitude']
    )['dropoff_location_id']
    df['dropoff_location_id'] = list(
        zip(df['dropoff_latitude'], df['dropoff_longitude'])
    )
    df['dropoff_location_id'] = df['dropoff_location_id'].map(dropoff_key)

    # Now just select all ID columns and metrics
    fact_table = df[[
        'VendorID',
        'datetime_id',
        'passenger_count_id',
        'trip_distance_id',
        'rate_code_id',
        'store_and_fwd_flag',
        'pickup_location_id',
        'dropoff_location_id',
        'payment_type_id',
        'fare_amount',
        'extra',
        'mta_tax',
        'tip_amount',
        'tolls_amount',
        'improvement_surcharge',
        'total_amount',
    ]].copy()

    return {
    'datetime_dim':     datetime_dim.to_dict(orient='records'),
    'passenger_count_dim': passenger_count_dim.to_dict(orient='records'),
    'trip_distance_dim':   trip_distance_dim.to_dict(orient='records'),
    'rate_code_dim':       rate_code_dim.to_dict(orient='records'),
    'pickup_location_dim': pickup_location_dim.to_dict(orient='records'),
    'dropoff_location_dim':dropoff_location_dim.to_dict(orient='records'),
    'payment_type_dim':    payment_type_dim.to_dict(orient='records'),
    # For the fact table, if it's still too big, process in chunks:
    'fact_table': [
        chunk.to_dict(orient='records')
        for _, chunk in fact_table.groupby(
            fact_table.index // 10000  # 10k rows per chunk
        )
    ],
    }



@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'