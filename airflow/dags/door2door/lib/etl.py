from typing import Final, Optional, List, Dict
import os
import awswrangler as wr
import awswrangler.exceptions 
import pandas as pd
import boto3
import logging
from clickhouse_driver import Client

LOKA_BUCKET: Final = "de-tech-assessment-2022"
RAW_BUCKET: Final = f"s3://{os.getenv('RAW_BUCKET')}"#"s3://raw-bucket-loka-challenge"
STAGING_BUCKET: Final = f"s3://{os.getenv('STAGING_BUCKET')}"#"s3://staging-bucket-loka-challenge"
CLICKHOUSE_HOST: Final = "host.docker.internal"

### TOP LEVEL FUNCTIONS 
# take events from specific day from loka bucket into raw_bucket 
def parse_events(**context) -> bool:
    """take events from specific day from loka bucket into raw_bucket

    Returns:
        bool: If there is data to process, returns True, otherwise
        returns False and downstream tasks are skipped
    """

    execution_date = context.get("ds")
    logging.info(f"### EXECUTION_DATE: {execution_date}")
    # Method 1 
    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(LOKA_BUCKET)  # type: ignore

    raw_events_list = []
    objects_list = bucket.objects.filter(Prefix=f"data/{execution_date}")
    
    for object in objects_list:
        obj = s3_client.get_object(Bucket=LOKA_BUCKET, Key=object.key)
        raw_events_list.append(
            pd.read_json(obj['Body'], lines=True)
        )

    if not raw_events_list:
        return False

    raw_events = pd.concat(raw_events_list)

    raw_events['date'] = execution_date
     # type: ignore
    logging.info(RAW_BUCKET)
    output = wr.s3.to_parquet(
        df=raw_events,
        path=RAW_BUCKET,
        dataset=True,
        mode='overwrite_partitions',
        partition_cols=['date', 'event']
    )

    logging.info(output)
    return True

# process raw events and flatten data structure 
def process_events(event_type: str, **context):
    """router for event processing

    Args:
        event_type (str): type of event

    Raises:
        ValueError: In case event type passed is not defined
    """
    date = context["ds"]
    logging.info(f"Processing {event_type} events from {date}")
    if event_type == "create":
        process_create_events(date)
    elif event_type == "delete":
        process_delete_events(date)
    elif event_type == "deregister":
        process_deregister_events(date)
    elif event_type == "register":
        process_register_events(date)
    elif event_type == "update":
        process_update_events(date)
    else:
        raise ValueError(f"event_type {event_type} is not defined")

# run query in clickhouse and write output to table partition
def write_query_result_to_table(
        query_path: str, 
        table_name: str, 
        **context
    ) -> None:
    """writes query output to table partition, being the execution
    date the partition

    Args:
        query_path (str): path to .sql query
        table_name (str): destination table
    """
    date = context.get("ds")
    with open(query_path, "r") as f:
        sql = f.read().format(date=date)
    write_query_to_dw(sql, table_name, date)

### FUNCTION HANDLERS
def process_create_events(date: str) -> None:
    table_name = "create_events"
    cols_to_drop = ["data", "event", "on"]
    cols_renaming_map = {
        "at": "created_at",
        "id": "operating_period_id",
    }
    cols_to_dt = ["created_at", "start", "finish"]
    _raw_to_stg_etl("create", table_name, date, cols_to_drop, cols_renaming_map, cols_to_dt)
    return

def process_delete_events(date: str) -> None:
    table_name = "delete_events"
    cols_to_drop = ["data", "event", "on"]
    cols_renaming_map = {
        "at": "deleted_at",
        "id": "operating_period_id",
    }
    cols_to_dt = ["deleted_at", "start", "finish"]
    _raw_to_stg_etl("delete", table_name, date, cols_to_drop, cols_renaming_map, cols_to_dt)
    return

def process_deregister_events(date: str) -> None:
    table_name = "deregister_events"
    cols_to_drop = ["data", "event", "on"]
    cols_rename_map = {
         "at": "deregisted_at",
        "id": "vehicle_id",
    }
    cols_to_dt = ["deregisted_at"]
    _raw_to_stg_etl("deregister", table_name, date, cols_to_drop, cols_rename_map, cols_to_dt)
    return

def process_register_events(date: str) -> None:
    table_name = "register_events"
    cols_to_drop = ["data", "event", "on"]
    cols_rename_map = {
         "at": "registed_at",
        "id": "vehicle_id",
    }
    cols_to_dt = ["registed_at"]
    _raw_to_stg_etl("register", table_name, date, cols_to_drop, cols_rename_map, cols_to_dt)
    return

def process_update_events(date: str) -> None:
    table_name = 'update_events'
    cols_to_drop = ["data", "on", "event", "location.at"]
    cols_rename_map = {
        "at": "updated_at",
        "id": "vehicle_id",
        "location.lat": "lat",
        "location.lng": "long",
    }
    cols_to_dt = ["updated_at"]
    _raw_to_stg_etl("update", table_name, date, cols_to_drop, cols_rename_map, cols_to_dt)
    return

def write_query_to_dw(query: str, table_name: str, date_partition: Optional[str] = ""):
    client = Client(CLICKHOUSE_HOST, port=9000)
    drop_partition_query = f"ALTER TABLE {table_name} DROP PARTITION '{date_partition}';"
    if date_partition != "":
        client.execute(drop_partition_query)
    client.execute(query)

def _raw_to_stg_etl(
        event_type: str, 
        table_name: str, 
        date: str,
        cols_to_drop: Optional[List[str]] = [],
        cols_renaming_mapping: Optional[Dict[str, str]] = {},
        cols_dt: Optional[List[str]] = [],
    ):
    """contains all the steps from the raw to stg etl, 
    written in a generic way so that it can be used by all events


    Args:
        event_type (str): event name
        table_name (str): table name for event
        date (str): execution date
        cols_to_drop (Optional[List[str]], optional): Cols to be dropped from event data. Defaults to [].
        cols_renaming_mapping (Optional[Dict[str, str]], optional): Col renaming mapping. Defaults to {}.
        cols_dt (Optional[List[str]], optional): Cols that need to be converted to datetime objects. Defaults to [].
    """
    input: pd.DataFrame = _read_event_type_raw(
        date=date, 
        event=event_type
    ) 

    if input.empty:
        logging.info(f"event {event_type} has no data to process")
        return

    processed = _flatten_data(input)

    # custom transformation
    if cols_to_drop and cols_renaming_mapping:
        processed = processed.drop(cols_to_drop, axis=1).rename(cols_renaming_mapping, axis=1)
    logging.info(processed.dtypes)
    processed = processed.astype(object)
    # convert cols to dt
    if cols_dt:
        for col in cols_dt:
            logging.info(f"converting {col}")
            processed[col] = pd.to_datetime(processed[col])

    logging.info(processed.dtypes)
    # write to dw
    client = Client('host.docker.internal', port=9000, settings={'use_numpy': True, "use_pandas": True})
    client.execute(f"ALTER TABLE {table_name} DROP PARTITION '{date}';")
    client.insert_dataframe(f'INSERT INTO {table_name} VALUES', processed)

    # write to s3
    _write_to_staging(df=processed, path=table_name)
    return 

# helper functions

def _flatten_data(df: pd.DataFrame):
    return pd.merge(
        df.reset_index(drop=True),
        pd.json_normalize(df.data),
        right_index=True,
        left_index=True
    )

def _read_event_type_raw(date: str, event: str) -> pd.DataFrame:
    logging.info(f"# extracting {event} event from {date}")
    try:
        files = wr.s3.read_parquet(
            path=RAW_BUCKET,
            dataset=True,
            partition_filter=(
                lambda x: 
                    True 
                    if x["date"] == date and x["event"] == event 
                    else False
            )
        )
    except awswrangler.exceptions.NoFilesFound as e:
        logging.info(e)
        files = pd.DataFrame()
    return files

def _write_to_staging(df: pd.DataFrame, path: str):
    return wr.s3.to_parquet(
        df=df,
        path=STAGING_BUCKET + f"/{path}",
        dataset=True,
        mode='overwrite_partitions',
        partition_cols=['date']
    )