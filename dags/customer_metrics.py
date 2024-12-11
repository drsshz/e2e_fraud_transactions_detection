from airflow.decorators import dag, task
from datetime import datetime
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import RenderConfig
from include.dbt.fraud.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from airflow.models.baseoperator import chain

AIRBYTE_JOB_ID_LOAD_CUSTOMER_TRANSACTIONS_RAW = "b36e5b14-6626-4ccf-95bc-d6a3de822af8"
AIRBYTE_JOB_ID_LOAD_LABELED_TRANSACTIONS_RAW = "2ea29b56-861a-40cf-a01b-64658ecf7dc2"
AIRBYTE_JOB_ID_RAW_TO_STAGING = "c70fc1a5-0011-45df-9aeb-a1e3609f964f"

@dag(
    start_date=datetime(2024,1,1),
    schedule="@daily",
    catchup=False,
    tags=["airbyte", "risk"]
)
def customer_metrics():
    load_customer_transactions_raw = AirbyteTriggerSyncOperator(
        task_id="load_customer_transactions_raw",
        airbyte_conn_id="airbyte",
        connection_id=AIRBYTE_JOB_ID_LOAD_CUSTOMER_TRANSACTIONS_RAW
    )
    load_labeled_transactions_raw = AirbyteTriggerSyncOperator(
        task_id="load_labeled_transactions_raw",
        airbyte_conn_id="airbyte",
        connection_id=AIRBYTE_JOB_ID_LOAD_CUSTOMER_TRANSACTIONS_RAW
    )
    write_to_staging = AirbyteTriggerSyncOperator(
        task_id="write_to_staging",
        airbyte_conn_id="airbyte",
        connection_id=AIRBYTE_JOB_ID_RAW_TO_STAGING
    )
    @task
    def airbyte_jobs_done():
        return True
    
    @task.external_python("/opt/airflow/soda_venv/bin/python")
    def audit_customer_transactions(
        scan_name="customer_transactions",
        check_subpaths="tables",
        data_source="staging"
        ):
        from include.soda.helpers import check
        check(scan_name, check_subpaths, data_source)
        
    @task.external_python("/opt/airflow/soda_venv/bin/python")
    def audit_labeled_transactions(
        scan_name="labeled_transactions",
        check_subpaths="tables",
        data_source="staging"
        ):
        from include.soda.helpers import check
        check(scan_name, check_subpaths, data_source)
    
    @task
    def quality_checks_done():
        return True
    
    publish = DbtTaskGroup(
        group_id="publish",
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=["path:models"]
        )
    )
    chain(
        [load_customer_transactions_raw, load_labeled_transactions_raw],
        write_to_staging,
        airbyte_jobs_done(),
        [audit_customer_transactions(), audit_labeled_transactions()],
        quality_checks_done(),
        publish
    )

customer_metrics()    