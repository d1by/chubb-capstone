from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="capstone_databricks_pipeline",
    start_date=datetime(2026, 1, 4),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
) as dag:

    run_capstone_pipeline = DatabricksSubmitRunOperator(
        task_id="run_capstone_pipeline",
        databricks_conn_id="databricks_default",
        tasks=[
            {
                "task_key": "bronze",
                "notebook_task": {
                    "notebook_path": "/Workspace/Users/REDACTEDEMAIL@MAIL.COM/capstone/bronze"
                }
            },
            {
                "task_key": "silver",
                "depends_on": [{"task_key": "bronze"}],
                "notebook_task": {
                    "notebook_path": "/Workspace/Users/REDACTEDEMAIL@MAIL.COM/capstone/silver"
                }
            },
            {
                "task_key": "gold",
                "depends_on": [{"task_key": "silver"}],
                "notebook_task": {
                    "notebook_path": "/Workspace/Users/REDACTEDEMAIL@MAIL.COM/capstone/gold"
                }
            }
        ],
    )