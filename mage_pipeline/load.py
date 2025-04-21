@data_exporter
def export_data_to_big_query(data, **kwargs) -> None:
    from mage_ai.settings.repo import get_repo_path
    from mage_ai.io.bigquery import BigQuery
    from mage_ai.io.config import ConfigFileLoader
    from pandas import DataFrame
    from os import path

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    # Define table IDs for all dimensions and fact table
    table_ids = {
        'datetime_dim': 'uberdataproject-457209.uber_dataset.datetime_dim',
        'passenger_count_dim': 'uberdataproject-457209.uber_dataset.passenger_count_dim',
        'trip_distance_dim': 'uberdataproject-457209.uber_dataset.trip_distance_dim',
        'rate_code_dim': 'uberdataproject-457209.uber_dataset.rate_code_dim',
        'pickup_location_dim': 'uberdataproject-457209.uber_dataset.pickup_location_dim',
        'dropoff_location_dim': 'uberdataproject-457209.uber_dataset.dropoff_location_dim',
        'payment_type_dim': 'uberdataproject-457209.uber_dataset.payment_type_dim',
        'fact_table': 'uberdataproject-457209.uber_dataset.fact_table'
    }

    for key in data.keys():
        # Handle fact table chunks
        if key == 'fact_table':
            # Combine all chunks from fact table
            fact_rows = [row for chunk in data[key] for row in chunk]
            df = DataFrame(fact_rows)
        else:
            # Convert dimension data to DataFrame
            df = DataFrame(data[key])

        # Export to BigQuery
        BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
            df,
            table_ids[key],
            if_exists='replace',  # Overwrite existing data
        )