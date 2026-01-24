from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from os import path
import pandas as pd

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_big_query(data, **kwargs) -> None:
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    # Debug: check what the exporter is receiving
    print("Exporter received type:", type(data))
    if isinstance(data, dict):
        print("Exporter received keys:", list(data.keys()))
    else:
        print("Exporter received non-dict input. Will export as fact_table only.")

    # If Mage passes a single DataFrame instead of dict, still handle it
    if isinstance(data, pd.DataFrame):
        data = {"fact_table": data}

    for key, df in data.items():
        table_id = f"uber-data-engineering-project2.uber_dataset_urmi.{key}"

        BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
            df,
            table_id,
            if_exists="replace",
        )
