from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import ShortCircuitOperator
from door2door.lib.etl import (
    parse_events,
    process_events,
    write_query_result_to_table
)
from datetime import datetime

EVENT_TYPES = ["create", "delete", "deregister", "register", "update"]
AIRFLOW_PATH = "/opt/airflow/dags"

with DAG("door2door_daily",
    "door2door Daily event processor DAG",
    schedule="@daily",
    start_date=datetime(2019,6,1),
    end_date=datetime(2019,6,4),
    max_active_runs=1,
    catchup=True
    ):

    parse_daily_events_task = ShortCircuitOperator(
        task_id="parse_daily_events",
        python_callable=parse_events
    )

    write_metadata_to_dw = PythonOperator(
        task_id="write_ops_metadata_to_dw",
        python_callable=write_query_result_to_table,
        op_kwargs={
            "query_path": f"{AIRFLOW_PATH}/door2door/queries/operation_metadata.sql",
            "table_name": "vehicle_operation_metadata"
        }
    )

    write_vehicle_pos_to_dw = PythonOperator(
        task_id="write_vehicle_pos_to_dw",
        python_callable=write_query_result_to_table,
        op_kwargs={
            "query_path": f"{AIRFLOW_PATH}/door2door/queries/vehicle_position.sql",
            "table_name": "vehicle_position_data"
        }
    )

    wait_for_completion = DummyOperator(
        task_id="wait_for_completion"
    )

    for event_type in EVENT_TYPES:
        process_daily_event_task = PythonOperator(
            task_id=f"process_{event_type}_event",
            python_callable=process_events,
            op_kwargs={
                "event_type": event_type
            },
            retries=2
        )
        parse_daily_events_task.set_downstream(process_daily_event_task)
        process_daily_event_task.set_downstream(wait_for_completion)
        process_daily_event_task.set_downstream(wait_for_completion)
    
    wait_for_completion.set_downstream(write_metadata_to_dw)
    wait_for_completion.set_downstream(write_vehicle_pos_to_dw)

