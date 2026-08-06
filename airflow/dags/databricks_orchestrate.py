# pyright: reportMissingImports=false
import time
from datetime import datetime
from airflow.sdk import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from databricks.sdk import WorkspaceClient

import os

ws = WorkspaceClient(
    host="https://dbc-b5261d5e-3ba2.cloud.databricks.com",
    token=os.environ.get("DATABRICKS_TOKEN", "your-default-dev-token")
)

@task
def trigger_databricks_job():
    job_trigger = ws.jobs.run_now(
        job_id="686816196398445"
    )
    return job_trigger.run_id

@task
def check_databricks_job_status(databricks_run_id):
    TERMINAL_STATES = ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]

    job_status = ws.jobs.get_run(run_id=databricks_run_id)

    # life_cycle_state tells us if the job is still running (result_state is None until terminal)
    while job_status.state.life_cycle_state.value not in TERMINAL_STATES:
        print(f"Job is still running. Life cycle state: {job_status.state.life_cycle_state.value}")
        time.sleep(10)
        job_status = ws.jobs.get_run(run_id=databricks_run_id)

    # Now the job has reached a terminal state — check the result
    result = job_status.state.result_state.value  # e.g. "SUCCESS", "FAILED", "CANCELED"
    print(f"Job finished with result: {result}")

    if result != "SUCCESS":
        raise Exception(f"Databricks job failed with result state: {result}")



@dag(
    schedule="0 6 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["databricks"],
)
def databricks_orchestration():
    job_trigger = trigger_databricks_job()
    status = check_databricks_job_status(job_trigger)

    trigger_dbt_workflow = TriggerDagRunOperator(
        task_id="trigger_dbt_workflow",
        trigger_dag_id="orchestrate",
        wait_for_completion=True,
        poke_interval=30,
    )

    # If check_databricks_job_status raises an exception, trigger_dbt_workflow won't run
    status >> trigger_dbt_workflow

databricks_orchestration()