# pyrefly: ignore [missing-import]
from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator

@dag
def orchestrate():

    @task
    def ingest_cdc():
        return "CDC data igested"

    @task.bash
    def clean_target():
        return "cd /opt/airflow/walmart_project && rm -rf target/* && rm -rf logs/*"

    @task.bash
    def source_freshness():
        #Manually setting the working directory using 'cd' command is not a good practice
        #It is better to use os.chdir() or set the working_directory parameter in the task
        return "cd /opt/airflow/walmart_project && dbt source freshness"


    
    silver_technical = BashOperator(
        task_id="silver_technical",
        bash_command="cd /opt/airflow/walmart_project && dbt run --select silver_technical"
    )

    silver_technical_tests = BashOperator(
        task_id="silver_technical_tests",
        bash_command="cd /opt/airflow/walmart_project && dbt test --select silver_technical"
    )

    silver_business = BashOperator(
        task_id="silver_business",
        bash_command="cd /opt/airflow/walmart_project && dbt run --select silver_business"
    )

    silver_business_tests = BashOperator(
        task_id="silver_business_tests",
        bash_command="cd /opt/airflow/walmart_project && dbt test --select silver_business"
    )

    gold_ephemeral_layer = BashOperator(
        task_id="gold_ephemeral_layer",
        bash_command="cd /opt/airflow/walmart_project && dbt run --select gold/ephemeral"
    )

    gold_dim_tables = BashOperator(
        task_id="gold_dim_tables",
        cwd = "/opt/airflow/walmart_project",
        bash_command="dbt snapshot"
    )

    gold_fact_tables = BashOperator(
        task_id="gold_fact_tables",
        bash_command="cd /opt/airflow/walmart_project && dbt run --select gold/fact_tables"
    )


    
    ingest_cdc() >> clean_target() >> source_freshness() >> silver_technical >> silver_technical_tests >> silver_business >> silver_business_tests >> gold_ephemeral_layer >> gold_dim_tables >> gold_fact_tables



orchestrate()


# pyright: reportMissingImports=false